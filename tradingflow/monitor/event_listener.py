import asyncio
import logging
import time
from datetime import datetime, timezone

import web3
from sqlalchemy.orm import Session
from web3.exceptions import BlockNotFound
from web3.types import LogReceipt  # Correct type hint for logs

from tradingflow.account_manager.common.logging_config import setup_logging
from tradingflow.common import config
from tradingflow.common.config import CONFIG
from tradingflow.common.db import db_session
from tradingflow.common.db.models.event import ListenerState
from tradingflow.common.db.models.monitored_contract import MonitoredContract
from tradingflow.common.db.services.contract_event_service import ContractEventService
from tradingflow.common.exceptions import DuplicateResourceException
from tradingflow.common.utils import eth_util

# Setup logging
setup_logging(CONFIG)

logger = logging.getLogger(__name__)

# --- Global Cache ---
# Cache monitored contracts to avoid frequent DB queries within a loop iteration
# Structure: {chain_id: {contract_address: MonitoredContract}}
monitored_contracts_cache = {}
# Cache ABI topic maps: {abi_name: {topic_hash: event_abi}}
event_topic_maps_cache = {}
# Cache contract ABIs: {abi_name: full_abi}
contract_abi_cache = {}
# Cache Contract factories: {chain_id: {contract_address: Contract}}
contract_factories_cache = {}
# --- End Global Cache ---


# 需要添加一个函数来处理二进制数据转换
def convert_binary_to_hex(obj):
    """
    Recursively convert binary data to hex strings for JSON serialization

    Args:
        obj: Any object potentially containing binary data

    Returns:
        Object with all binary data converted to hex strings
    """
    if isinstance(obj, bytes):
        return "0x" + obj.hex()
    elif isinstance(obj, list):
        return [convert_binary_to_hex(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_binary_to_hex(value) for key, value in obj.items()}
    else:
        return obj


async def update_monitored_contracts_cache(db: Session):
    """Updates the global cache of monitored contracts from the database."""
    global monitored_contracts_cache
    logger.info("Updating monitored contracts cache...")
    contracts = db.query(MonitoredContract).all()
    new_cache = {}
    for contract in contracts:
        if contract.chain_id not in new_cache:
            new_cache[contract.chain_id] = {}

        # Create a copy of the object (dictionary) instead of storing ORM object
        contract_dto = {
            "id": contract.id,
            "contract_address": contract.contract_address,
            "chain_id": contract.chain_id,
            "contract_type": contract.contract_type,
            "abi_name": contract.abi_name,
            "added_at": contract.added_at,
        }

        new_cache[contract.chain_id][contract.contract_address.lower()] = contract_dto

        # Ensure the contract's ABI is loaded and cached
        abi_name = contract.abi_name
        if abi_name not in contract_abi_cache:
            abi = eth_util.load_abi(abi_name)
            if abi:
                contract_abi_cache[abi_name] = abi
                # Also update event topic mapping cache
                event_topic_maps_cache[abi_name] = eth_util.get_event_topic_map(abi)
            else:
                logger.warning(
                    "Could not load ABI '%s' for contract %s",
                    abi_name,
                    contract.contract_address,
                )

    monitored_contracts_cache = new_cache
    logger.info(
        "Cache updated with %d contracts across %d chains.",
        len(contracts),
        len(new_cache),
    )


async def get_last_processed_block(db: Session, chain_id: int) -> int:
    """Gets the last processed block number for a chain, or returns -1 if not found."""
    state = db.query(ListenerState).filter(ListenerState.chain_id == chain_id).first()
    if state:
        return state.last_processed_block
    else:
        # If no state, maybe start from current block - N or a fixed historical block
        # For simplicity, let's start from 0, meaning it will process from genesis
        # A better approach is to get current block and start from current - confirmations
        logger.warning(
            "No listener state found for chain %d. Starting from block 0.", chain_id
        )
        return -1  # Indicate no prior state


async def update_last_processed_block(db: Session, chain_id: int, block_number: int):
    """Updates the last processed block number for a chain."""
    state = db.query(ListenerState).filter(ListenerState.chain_id == chain_id).first()
    if state:
        state.last_processed_block = block_number
    else:
        state = ListenerState(chain_id=chain_id, last_processed_block=block_number)
        db.add(state)
    try:
        db.commit()
    except Exception as e:
        logger.error("Error committing listener state for chain %d: %s", chain_id, e)
        db.rollback()


async def get_contract_instance(w3, contract_address, abi_name):
    """Get or create contract instance"""
    # Get chain ID asynchronously
    chain_id = int(await w3.eth.chain_id)

    # Initialize cache for this chain ID
    if chain_id not in contract_factories_cache:
        contract_factories_cache[chain_id] = {}

    # Check if we've already cached this contract instance
    if contract_address in contract_factories_cache[chain_id]:
        return contract_factories_cache[chain_id][contract_address]

    # Get ABI
    abi = contract_abi_cache.get(abi_name)
    if not abi:
        logger.warning(
            "Could not get ABI '%s' for contract %s", abi_name, contract_address
        )
        return None

    # Create and cache contract instance
    try:
        checksum_address = web3.Web3.to_checksum_address(contract_address)
        contract = w3.eth.contract(address=checksum_address, abi=abi)
        contract_factories_cache[chain_id][contract_address] = contract
        return contract
    except Exception as e:
        logger.error("Error creating contract instance: %s", e)
        return None


async def process_chain_events(db: Session, chain_id: int):
    """Processes events for a single chain."""
    w3 = eth_util.get_w3_instance(chain_id)
    if not w3:
        logger.error("Skipping chain %d: Could not get Web3 instance.", chain_id)
        return

    contracts_on_chain = monitored_contracts_cache.get(chain_id, {})
    if not contracts_on_chain:
        logger.info("Skipping chain %d: No contracts to monitor.", chain_id)
        return
    logger.info(
        "Chain %d: Found %d contracts to monitor. vault_addresses:\n%s",
        chain_id,
        len(contracts_on_chain),
        contracts_on_chain.keys(),
    )
    try:
        # 1. Determine block range
        last_processed = await get_last_processed_block(db, chain_id)
        latest_block_num = await w3.eth.get_block_number()
        to_block = latest_block_num - config.CONFIG.get("BLOCK_CONFIRMATIONS")
        from_block = last_processed + 1

        if from_block > to_block:
            logger.info(
                "Chain %d: No new blocks to process (from=%d, to=%d).",
                chain_id,
                from_block,
                to_block,
            )
            return

        logger.info(
            "Chain %d: Processing blocks %d to %d...", chain_id, from_block, to_block
        )

        # 2. Prepare filter parameters
        addresses_to_monitor = list(contracts_on_chain.keys())
        # Convert to checksum addresses
        checksum_addresses = eth_util.normalize_addresses(addresses_to_monitor, w3)
        all_event_topics = []  # Collect all topics from all relevant ABIs
        for contract_info in contracts_on_chain.values():
            # Use dictionary access instead of attribute access
            abi_name = contract_info["abi_name"]
            abi = contract_abi_cache.get(abi_name)
            if not abi:
                abi = eth_util.load_abi(abi_name)
                if abi:
                    contract_abi_cache[abi_name] = abi

            if abi:
                if abi_name not in event_topic_maps_cache:
                    event_topic_maps_cache[abi_name] = eth_util.get_event_topic_map(abi)
                all_event_topics.extend(event_topic_maps_cache[abi_name].keys())
            else:
                logger.warning(
                    "Could not load ABI '%s' for %s",
                    abi_name,
                    contract_info["contract_address"],
                )

        unique_topics = list(set(all_event_topics))
        if not unique_topics:
            logger.warning(
                "Chain %d: No event topics found for monitored contracts. Skipping get_logs.",
                chain_id,
            )
            await update_last_processed_block(
                db, chain_id, to_block
            )  # Still advance block num
            return

        # 3. Fetch logs - using checksum addresses
        logs: list[LogReceipt] = await w3.eth.get_logs(
            {
                "fromBlock": from_block,
                "toBlock": to_block,
                "address": checksum_addresses,  # Use checksum addresses
                "topics": [
                    unique_topics
                ],  # Pass topics as list within a list for 'OR' logic
            }
        )

        if not logs:
            logger.info(
                "Chain %d: No relevant events found in blocks %d-%d.",
                chain_id,
                from_block,
                to_block,
            )
            await update_last_processed_block(db, chain_id, to_block)
            return

        logger.info("Chain %d: Found %d potential events.", chain_id, len(logs))

        # 4. Process logs
        event_count = 0
        success_count = 0
        duplicate_count = 0
        error_count = 0
        processed_log_keys = set()  # Track (tx_hash, log_index) to handle duplicates

        # Fetch block timestamps efficiently (consider batching if many blocks)
        block_timestamps = {}
        unique_block_nums = sorted(list(set(log["blockNumber"] for log in logs)))
        for block_num in unique_block_nums:
            try:
                block = await w3.eth.get_block(block_num)
                block_timestamps[block_num] = datetime.fromtimestamp(
                    block["timestamp"], tz=timezone.utc
                )
            except BlockNotFound:
                logger.warning(
                    "Chain %d: Block %d not found while fetching timestamp.",
                    chain_id,
                    block_num,
                )
                block_timestamps[block_num] = None  # Handle missing timestamp later
            except Exception as e:
                logger.error(
                    "Chain %d: Error fetching block %d: %s", chain_id, block_num, e
                )
                block_timestamps[block_num] = None

        # Create event service instance
        event_service = ContractEventService()

        for log in logs:
            log_key = (log["transactionHash"].hex(), log["logIndex"])
            if log_key in processed_log_keys:
                continue
            processed_log_keys.add(log_key)
            event_count += 1

            contract_address_lower = log["address"].lower()
            contract_info = contracts_on_chain.get(contract_address_lower)

            if not contract_info:  # Should not happen if get_logs address filter works
                logger.warning(
                    "Chain %d: Log received for unknown address %s",
                    chain_id,
                    log["address"],
                )
                continue

            abi_name = contract_info["abi_name"]
            topic_map = event_topic_maps_cache.get(abi_name)

            if not topic_map:
                logger.warning(
                    "Missing topic map for ABI '%s', skipping log.", abi_name
                )
                continue

            event_signature_topic = log["topics"][0] if log["topics"] else None
            event_abi = topic_map.get(event_signature_topic)

            if not event_abi:
                # This might be an event from the contract we don't explicitly track
                logger.debug(
                    "Chain %d: Unknown event topic %s from %s",
                    chain_id,
                    event_signature_topic.hex(),
                    contract_address_lower,
                )
                continue

            try:
                # Get contract instance for properly decoding events
                contract = await get_contract_instance(w3, log["address"], abi_name)
                if not contract:
                    logger.warning(
                        "Could not get contract instance for address %s", log["address"]
                    )
                    continue

                # Use contract instance to properly decode event
                event_name = event_abi["name"]

                # More accurate event decoding method
                decoded_event = contract.events[event_name]().process_log(log)
                logger.debug(
                    "Chain %d: Decoded event %s from log %s",
                    chain_id,
                    event_name,
                    log_key,
                )
                # event_args = dict(decoded_event.args)
                event_args = convert_binary_to_hex(dict(decoded_event.args))
                logger.debug(
                    "Chain %d: Event args for %s: %s", chain_id, event_name, event_args
                )

                # Extract user address (optional, based on event type)
                user_addr = None
                if event_name == "Deposit" and "owner" in event_args:
                    user_addr = event_args["owner"]
                elif event_name == "Withdraw" and "owner" in event_args:
                    user_addr = event_args["owner"]
                elif event_name == "Transfer" and "from" in event_args:
                    user_addr = event_args["from"]
                elif event_name == "SignalReceived" and "investor" in event_args:
                    user_addr = event_args["investor"]

                block_time = block_timestamps.get(log["blockNumber"])
                if block_time is None:
                    logger.warning(
                        "Chain %d: Missing timestamp for block %d, skipping event %s",
                        chain_id,
                        log["blockNumber"],
                        log_key,
                    )
                    continue

                # Prepare event data for ContractEventService
                event_data = {
                    "transaction_hash": log["transactionHash"].hex(),
                    "log_index": log["logIndex"],
                    "block_number": log["blockNumber"],
                    "block_timestamp": block_time,
                    "chain_id": chain_id,
                    "contract_address": log["address"],
                    "event_name": event_name,
                    "user_address": user_addr,
                    "parameters": event_args,  # Pass event arguments as parameters
                }

                # Use ContractEventService to create the event
                try:
                    event_service.create_event(db, event_data)
                    success_count += 1
                except DuplicateResourceException:
                    duplicate_count += 1
                    logger.debug(
                        "Chain %d: Duplicate event found for tx hash %s and log index %s",
                        chain_id,
                        event_data["transaction_hash"],
                        event_data["log_index"],
                    )
                except Exception as e:
                    error_count += 1
                    logger.error("Chain %d: Error saving event: %s", chain_id, str(e))

            except Exception as e:
                error_count += 1
                logger.error(
                    "Chain %d: Error decoding/processing log %s: %s",
                    chain_id,
                    log_key,
                    e,
                    exc_info=True,
                )

        logger.info(
            "Chain %d: Processed %d events. Success: %d, Duplicates: %d, Errors: %d",
            chain_id,
            event_count,
            success_count,
            duplicate_count,
            error_count,
        )

        # Update block number ONLY if processing was successful up to 'to_block'
        await update_last_processed_block(db, chain_id, to_block)

    except Exception as e:
        logger.error(
            "Chain %d: Unhandled error in processing loop: %s",
            chain_id,
            e,
            exc_info=True,
        )
        # Don't update block number on general failure


async def main_loop():
    """Main listener loop."""
    # Use shared module's db_session context manager
    with db_session() as db:
        try:
            await update_monitored_contracts_cache(db)  # Initial load
        except Exception as e:
            logger.error("Error during initial cache update: %s", e, exc_info=True)

    last_cache_update_time = time.time()

    while True:
        start_time = time.time()

        # Use shared module's db_session context manager
        with db_session() as db:
            try:
                # Periodically update monitored contracts cache
                if time.time() - last_cache_update_time > 30:  # Update every 30 seconds
                    await update_monitored_contracts_cache(db)
                    last_cache_update_time = time.time()

                # Process events for all configured and monitored chains concurrently
                tasks = [
                    process_chain_events(db, chain_id)
                    for chain_id in monitored_contracts_cache.keys()
                ]
                if tasks:
                    await asyncio.gather(*tasks)

            except Exception as e:
                logger.error("Error in main loop iteration: %s", e, exc_info=True)
        # Sleep before next iteration
        elapsed = time.time() - start_time
        sleep_duration = max(0, 15 - elapsed)
        logger.info(
            "Loop finished in %.2fs. Sleeping for %.2fs.", elapsed, sleep_duration
        )
        await asyncio.sleep(sleep_duration)


if __name__ == "__main__":
    logger.info("Starting Event Listener...")
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Listener stopped by user.")
