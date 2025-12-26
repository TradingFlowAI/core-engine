import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from .state_store import StateStoreFactory


class NodeTaskManager:
    """
    Node Task Manager: Manages all running node tasks.
    Designed to work in multi-process environment using shared state storage.

    Implemented as singleton pattern to ensure same instance is shared within a process.

    Attributes:
        state_store_type: State storage type (e.g., Redis)
        state_store_config: State storage configuration
        worker_id: Unique identifier for current worker process
        state_store: State storage instance, is a singleton instance

        _local_tasks: Local in-process node task mapping
        _initialized: Whether initialized
        _tasks_key_prefix: Key prefix for storing node task details
        _tasks_list_key: Key for storing all node task list
        _worker_tasks_prefix: Prefix for each worker's node task list
    """

    _instance = None

    @classmethod
    def get_instance(
        cls,
        state_store_type: str = "redis",
        state_store_config: Dict[str, Any] = None,
        worker_id: str = None,
    ):
        """Get singleton instance."""
        if cls._instance is None:
            cls._instance = cls(state_store_type, state_store_config, worker_id)
        return cls._instance

    def __init__(
        self,
        state_store_type: str = "redis",
        state_store_config: Dict[str, Any] = None,
        worker_id: str = None,
    ):
        # If instance already exists, return it (singleton pattern)
        if NodeTaskManager._instance is not None:
            return

        # Rest of initialization logic
        self.logger = logging.getLogger(__name__)
        self.state_store_type = state_store_type
        self.state_store_config = state_store_config or {}
        self.worker_id = worker_id
        self.state_store = None
        self._local_tasks = {}  # Local in-process node task mapping: task_id -> task_info
        self._initialized = False

        # Define storage key prefixes
        self._tasks_key_prefix = "node_tasks:"  # Individual node task details
        self._tasks_list_key = "node_tasks_list"  # All node task list
        self._worker_tasks_prefix = "worker_tasks:"  # Each worker's node task list

    async def initialize(self) -> bool:
        """Initialize node task manager and state storage."""
        if self._initialized:
            return True

        try:
            # Create state store - corrected invocation
            self.state_store = StateStoreFactory.create(
                self.state_store_type, self.state_store_config
            )

            # Initialize state store
            initialized = await self.state_store.initialize()
            if not initialized:
                self.logger.error("Failed to initialize state store")
                return False

            self._initialized = True
            self.logger.info(
                f"NodeTaskManager initialized with {self.state_store_type} state store"
            )
            return True

        except Exception as e:
            self.logger.exception(f"Error initializing NodeTaskManager: {str(e)}")
            return False

    async def register_task(self, node_task_id: str, task_info: Dict[str, Any]) -> bool:
        """
        Register node task to manager.

        Args:
            node_task_id: Node task ID
            task_info: Task info (contains type, status, start time, etc.)

        Returns:
            bool: Whether registration was successful
        """
        if not self._initialized:
            if not await self.initialize():
                return False

        try:
            # Add worker_id to task info
            if self.worker_id:
                task_info["worker_id"] = self.worker_id

            # Set task status and timestamp
            task_info["registered_at"] = datetime.now().isoformat()
            if "status" not in task_info:
                task_info["status"] = "registered"

            # Save to state storage
            task_key = f"{self._tasks_key_prefix}{node_task_id}"
            await self.state_store.set_value(task_key, task_info)

            # Add to task list
            await self.state_store.add_to_set(self._tasks_list_key, node_task_id)

            # If has worker_id, add to worker task list
            if self.worker_id:
                worker_key = f"{self._worker_tasks_prefix}{self.worker_id}"
                await self.state_store.add_to_set(worker_key, node_task_id)

            # Save to local cache
            self._local_tasks[node_task_id] = task_info

            self.logger.info(f"Node task {node_task_id} registered successfully")
            return True

        except Exception as e:
            self.logger.exception(
                f"Error registering node task {node_task_id}: {str(e)}"
            )
            return False

    async def update_task_status(
        self, node_task_id: str, status: str, additional_info: Dict[str, Any] = None
    ) -> bool:
        """
        Update node task status.

        Args:
            node_task_id: Node task ID
            status: New status
            additional_info: Additional info to update

        Returns:
            bool: Whether update was successful
        """
        if not self._initialized:
            if not await self.initialize():
                return False

        try:
            # Get current task info
            task_key = f"{self._tasks_key_prefix}{node_task_id}"
            task_info = await self.state_store.get_value(task_key)

            if not task_info:
                self.logger.warning(
                    f"Node task {node_task_id} not found, cannot update status"
                )
                return False

            # Update status and timestamp
            task_info["status"] = status
            task_info["updated_at"] = datetime.now().isoformat()

            # Add additional info
            if additional_info:
                task_info.update(additional_info)

            # Save back to state storage
            await self.state_store.set_value(task_key, task_info)

            # Update local cache (if exists)
            if node_task_id in self._local_tasks:
                self._local_tasks[node_task_id].update(task_info)

            self.logger.info(f"Node task {node_task_id} status updated to {status}")
            return True

        except Exception as e:
            self.logger.exception(
                f"Error updating node task {node_task_id} status: {str(e)}"
            )
            return False

    async def get_task(self, node_task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get node task details.

        Args:
            node_task_id: Node task ID

        Returns:
            Dict or None: Task info, None if not exists
        """
        if not self._initialized:
            if not await self.initialize():
                return None

        try:
            # Prefer getting from local cache
            if node_task_id in self._local_tasks:
                self.logger.debug("node_task_id in local cache")
                # But still check state storage to ensure data is up-to-date
                task_key = f"{self._tasks_key_prefix}{node_task_id}"
                stored_info = await self.state_store.get_value(task_key)
                self.logger.debug(f"stored_info: {stored_info}")

                if stored_info:
                    # Update local cache
                    self._local_tasks[node_task_id] = stored_info
                    return stored_info
                return self._local_tasks[node_task_id]

            # Get from state store
            task_key = f"{self._tasks_key_prefix}{node_task_id}"
            task_info = await self.state_store.get_value(task_key)
            self.logger.debug(f"task_info: {task_info}")

            if task_info:
                # Cache to local
                self._local_tasks[node_task_id] = task_info

            return task_info

        except Exception as e:
            self.logger.exception(f"Error getting node task {node_task_id}: {str(e)}")
            return None

    async def get_all_tasks(self) -> List[Dict[str, Any]]:
        """
        Get all node task info.

        Returns:
            List: Node task info list
        """
        if not self._initialized:
            if not await self.initialize():
                return []

        try:
            # Get all task IDs
            task_ids = await self.state_store.get_set_members(self._tasks_list_key)

            # Batch get task info
            tasks = []
            for task_id in task_ids:
                task_info = await self.get_task(task_id)
                if task_info:
                    tasks.append(task_info)

            return tasks

        except Exception as e:
            self.logger.exception(f"Error getting all node tasks: {str(e)}")
            return []

    async def get_worker_tasks(self, worker_id: str = None) -> List[Dict[str, Any]]:
        """
        Get all node tasks for specified worker.

        Args:
            worker_id: Optional worker ID, defaults to current worker_id

        Returns:
            List: Node task info list
        """
        if not self._initialized:
            if not await self.initialize():
                return []

        worker_id = worker_id or self.worker_id
        if not worker_id:
            self.logger.warning("No worker_id specified for get_worker_tasks")
            return []

        try:
            # Get worker's task list
            worker_key = f"{self._worker_tasks_prefix}{worker_id}"
            task_ids = await self.state_store.get_set_members(worker_key)

            # Batch get task info
            tasks = []
            for task_id in task_ids:
                task_info = await self.get_task(task_id)
                if task_info:
                    tasks.append(task_info)

            return tasks

        except Exception as e:
            self.logger.exception(f"Error getting worker tasks: {str(e)}")
            return []

    async def stop_task(self, node_task_id: str) -> bool:
        """
        Stop node task execution.
        Note: This method only sets termination flag, does not actually cancel task.
        Cancel logic should be handled by node task monitoring code after calling this method.

        Args:
            node_task_id: Node task ID

        Returns:
            bool: Whether termination flag was set successfully
        """
        if not self._initialized:
            if not await self.initialize():
                return False

        try:
            # Set termination flag
            await self.state_store.set_termination_flag(
                node_task_id,
                {
                    "reason": "Stopped by NodeTaskManager",
                    "timestamp": datetime.now().isoformat(),
                },
            )

            # Update task status
            await self.update_task_status(
                node_task_id,
                "stopping",
                {"termination_requested_at": datetime.now().isoformat()},
            )

            self.logger.info(f"Stop request sent to node task {node_task_id}")
            return True

        except Exception as e:
            self.logger.exception(f"Error stopping node task {node_task_id}: {str(e)}")
            return False

    async def remove_task(self, node_task_id: str) -> bool:
        """
        Remove node task from manager.

        Args:
            node_task_id: Node task ID

        Returns:
            bool: Whether removal was successful
        """
        if not self._initialized:
            if not await self.initialize():
                return False

        try:
            # Delete task info from state storage
            task_key = f"{self._tasks_key_prefix}{node_task_id}"
            await self.state_store.delete_value(task_key)

            # Remove from task list
            await self.state_store.remove_from_set(self._tasks_list_key, node_task_id)

            # If has worker_id, remove from worker task list
            if self.worker_id:
                worker_key = f"{self._worker_tasks_prefix}{self.worker_id}"
                await self.state_store.remove_from_set(worker_key, node_task_id)

            # Remove from local cache
            if node_task_id in self._local_tasks:
                del self._local_tasks[node_task_id]

            self.logger.info(f"Node task {node_task_id} removed successfully")
            return True

        except Exception as e:
            self.logger.exception(f"Error removing node task {node_task_id}: {str(e)}")
            return False

    async def cleanup_worker_tasks(self, worker_id: str = None) -> bool:
        """
        Cleanup all node task records for worker.

        Args:
            worker_id: Optional worker ID, defaults to current worker_id

        Returns:
            bool: Whether cleanup was successful
        """
        if not self._initialized:
            if not await self.initialize():
                return False

        worker_id = worker_id or self.worker_id
        if not worker_id:
            self.logger.warning("No worker_id specified for cleanup_worker_tasks")
            return False

        try:
            # Get worker's task list
            worker_key = f"{self._worker_tasks_prefix}{worker_id}"
            task_ids = await self.state_store.get_set_members(worker_key)

            # Batch update task status to terminated
            for task_id in task_ids:
                await self.update_task_status(
                    task_id,
                    "terminated",
                    {
                        "terminated_reason": "Worker shutdown",
                        "terminated_at": datetime.now().isoformat(),
                    },
                )

            # Clear worker task list
            await self.state_store.delete_value(worker_key)

            self.logger.info(f"All node tasks for worker {worker_id} cleaned up")
            return True

        except Exception as e:
            self.logger.exception(f"Error cleaning up worker tasks: {str(e)}")
            return False

    async def get_comprehensive_node_status(
        self,
        flow_id: str,
        cycle: int,
        node_ids: List[str] = None,
        include_logs: bool = True,
        include_signals: bool = True
    ) -> Dict[str, Dict]:
        """
        Get comprehensive status for all nodes in a flow/cycle including logs and signals

        Args:
            flow_id: Flow identifier
            cycle: Cycle number
            node_ids: Optional list of specific node IDs to query
            include_logs: Whether to include logs in the response (default True)
            include_signals: Whether to include signals in the response (default True)

        Returns:
            Dict mapping node_id to comprehensive status info including:
            - status: execution status
            - logs: recent logs (if include_logs=True)
            - signals: signal data (if include_signals=True)
            - metadata: additional execution info
        """
        try:
            import json
            from infra.db.services.flow_execution_log_service import FlowExecutionLogService

            # Get task data directly from Redis instead of memory
            flow_tasks = {}
            
            # Use Redis pattern matching to find all node tasks for this flow/cycle
            pattern = f"node_tasks:{flow_id}_{cycle}_*"
            task_keys = await self.state_store.redis_client.keys(pattern)
            
            self.logger.debug(f"Found {len(task_keys)} task keys for pattern {pattern}")
            
            for task_key in task_keys:
                try:
                    # Get task data from Redis
                    task_data_str = await self.state_store.redis_client.get(task_key)
                    if not task_data_str:
                        continue
                        
                    # Parse JSON data
                    task_info = json.loads(task_data_str)
                    
                    # Extract node_id from node_task_id (format: flow_id_cycle_node_id)
                    node_task_id = task_info.get('node_task_id', '')
                    if node_task_id:
                        # Parse node_id from node_task_id
                        parts = node_task_id.split('_')
                        if len(parts) >= 3:
                            # node_id is everything after flow_id_cycle_
                            node_id = '_'.join(parts[2:])
                            if node_id and (not node_ids or node_id in node_ids):
                                flow_tasks[node_id] = task_info
                                
                except Exception as e:
                    self.logger.warning(f"Error parsing task data from key {task_key}: {str(e)}")
                    continue

            comprehensive_status = {}
            
            # Only initialize services when needed
            log_service = None
            signal_service = None
            
            if include_logs:
                log_service = FlowExecutionLogService()
            
            if include_signals:
                try:
                    from infra.db.services.flow_execution_signal_service import (
                        FlowExecutionSignalService,
                    )
                    signal_service = FlowExecutionSignalService()
                except Exception as e:
                    self.logger.debug(f"Signal service not available: {e}")

            for node_id, task_info in flow_tasks.items():
                try:
                    # Only get logs when needed
                    logs_data = []
                    if include_logs and log_service:
                        logs_data = await log_service.get_logs_by_flow_cycle_node(
                            flow_id=flow_id,
                            cycle=cycle,
                            node_id=node_id,
                            limit=50,
                            order_by="created_at",
                            order_direction="desc"
                        )

                    # Only get signals when needed
                    signals = {'input': [], 'output': [], 'handles': {}}
                    if include_signals:
                        # First try to query from signal table
                        signal_from_db = False
                        if signal_service:
                            try:
                                node_signals = await signal_service.get_signals_by_node(
                                    flow_id=flow_id,
                                    cycle=cycle,
                                    node_id=node_id,
                                    limit=50
                                )
                                if node_signals.get('input') or node_signals.get('output'):
                                    # Convert database format to frontend expected format (align with WebSocket format)
                                    def convert_db_signal(db_sig: dict) -> dict:
                                        """Convert database signal format to frontend expected format."""
                                        direction = db_sig.get('direction', 'output')
                                        # handleId: input uses target_handle, output uses source_handle
                                        handle_id = db_sig.get('target_handle') if direction == 'input' else db_sig.get('source_handle')
                                        return {
                                            'timestamp': db_sig.get('created_at'),
                                            'cycle': db_sig.get('cycle'),
                                            'direction': direction,
                                            'fromNodeId': db_sig.get('from_node_id'),
                                            'toNodeId': db_sig.get('to_node_id'),
                                            'handleId': handle_id or db_sig.get('source_handle') or db_sig.get('target_handle'),
                                            'sourceHandle': db_sig.get('source_handle'),
                                            'targetHandle': db_sig.get('target_handle'),
                                            'dataType': (db_sig.get('signal_type') or 'unknown').replace('SignalType.', ''),
                                            'payload': db_sig.get('payload'),
                                        }
                                    
                                    input_signals = [convert_db_signal(s) for s in node_signals.get('input', [])]
                                    output_signals = [convert_db_signal(s) for s in node_signals.get('output', [])]
                                    
                                    # Build handles compatible format
                                    handles = {}
                                    for sig in input_signals + output_signals:
                                        if sig.get('handleId'):
                                            handles[sig['handleId']] = sig
                                    
                                    signals = {
                                        'input': input_signals,
                                        'output': output_signals,
                                        'handles': handles
                                    }
                                    signal_from_db = True
                            except Exception as sig_err:
                                self.logger.debug(f"Failed to query signals from DB: {sig_err}")
                        
                        # If no data in signal table, fallback to extracting from logs
                        if not signal_from_db:
                            # If logs already loaded, use directly
                            fallback_logs = logs_data
                            # If logs not loaded, temporarily get (only for signal extraction)
                            if not fallback_logs:
                                try:
                                    if not log_service:
                                        log_service = FlowExecutionLogService()
                                    fallback_logs = await log_service.get_logs_by_flow_cycle_node(
                                        flow_id=flow_id,
                                        cycle=cycle,
                                        node_id=node_id,
                                        limit=50,
                                        order_by="created_at",
                                        order_direction="desc"
                                    )
                                except Exception as log_err:
                                    self.logger.debug(f"Failed to get logs for signal fallback: {log_err}")
                            
                            if fallback_logs:
                                signals = self._extract_signals_from_logs(fallback_logs)

                    # Calculate execution time if available
                    execution_time = None
                    if task_info.get('start_time') and task_info.get('completed_at'):
                        try:
                            from datetime import datetime
                            start_time = datetime.fromisoformat(task_info['start_time'].replace('Z', '+00:00'))
                            completed_at = datetime.fromisoformat(task_info['completed_at'].replace('Z', '+00:00'))
                            execution_time = (completed_at - start_time).total_seconds()
                        except Exception:
                            pass

                    comprehensive_status[node_id] = {
                        'status': task_info.get('status', 'unknown'),
                        'logs': logs_data,
                        'signals': signals,
                        'metadata': {
                            'task_id': task_info.get('task_id'),
                            'worker_id': task_info.get('worker_id'),
                            'created_at': task_info.get('created_at'),
                            'updated_at': task_info.get('updated_at'),
                            'start_time': task_info.get('start_time'),
                            'completed_at': task_info.get('completed_at'),
                            'registered_at': task_info.get('registered_at'),
                            'execution_time': execution_time,
                            'progress': task_info.get('progress'),
                            'node_type': task_info.get('node_type'),
                            'component_id': task_info.get('component_id'),
                            'message': task_info.get('message'),
                            'error': task_info.get('error')
                        }
                    }

                except Exception as e:
                    self.logger.warning(f"Error getting comprehensive status for node {node_id}: {str(e)}")
                    # Fallback to basic task info
                    comprehensive_status[node_id] = {
                        'status': task_info.get('status', 'unknown'),
                        'logs': [],
                        'signals': {'input': [], 'output': [], 'handles': {}},
                        'metadata': {
                            'task_id': task_info.get('task_id'),
                            'node_type': task_info.get('node_type'),
                            'error': str(e)
                        }
                    }

            self.logger.info(f"Retrieved comprehensive status for {len(comprehensive_status)} nodes in flow {flow_id} cycle {cycle}")
            return comprehensive_status

        except Exception as e:
            self.logger.exception(f"Error getting comprehensive node status: {str(e)}")
            return {}

    def _extract_signals_from_logs(self, logs_data: list) -> dict:
        """
        DEPRECATED: Legacy fallback for extracting signal data from log metadata.
        
        Since signal data is now stored in the dedicated `flow_execution_signals` table,
        this function will typically return empty results. It remains as a fallback for
        historical data that may still have `signal_data` in log_metadata.

        Args:
            logs_data: List of log dictionaries

        Returns:
            dict: Extracted signals with structure:
                {
                    'input': [list of input signals],
                    'output': [list of output signals],
                    'handles': {handle: signal_data}  # Legacy format for compatibility
                }
        """
        result = {
            'input': [],
            'output': [],
            'handles': {}  # Legacy format
        }

        for log_dict in logs_data:
            # Extract signal routing info from log metadata (simplified, no payload)
            # Compatible with both field names: log_metadata and metadata
            log_metadata = log_dict.get('log_metadata') or log_dict.get('metadata') or {}
            event = log_metadata.get('event')
            
            # Only process signal events
            if event not in ('signal_sent', 'signal_received'):
                # Legacy fallback: check for old signal_data format
                if 'signal_data' not in log_metadata:
                    continue
                signal_data = log_metadata.get('signal_data')
                if not isinstance(signal_data, dict):
                    continue
            else:
                # New format: no payload, just routing info
                signal_data = {}
            
            target_handle = log_metadata.get('target_handle')
            source_handle = log_metadata.get('source_handle')
            source_node = log_metadata.get('source_node')
            signal_type = log_metadata.get('signal_type')
            
            # Construct signal entry from routing info
            handle_id = target_handle or source_handle or 'unknown'
            signal_entry = {
                'timestamp': log_dict.get('created_at'),
                'log_level': log_dict.get('log_level'),
                'handle_id': handle_id,
                'source_node': source_node,
                'source_handle': source_handle,
                'target_handle': target_handle,
                'signal_type': signal_type,
                # Note: payload not available in new format, query Signal table for full data
            }
            
            # Legacy format
            result['handles'][handle_id] = signal_entry
            
            # Determine direction
            if event == 'signal_received' or target_handle:
                signal_entry['direction'] = 'input'
                result['input'].append(signal_entry)
            else:
                signal_entry['direction'] = 'output'
                result['output'].append(signal_entry)

        return result

    async def close(self):
        """Close node task manager and state store."""
        if self.state_store:
            try:
                await self.state_store.close()
                self._initialized = False
                self.logger.info("NodeTaskManager closed")
            except Exception as e:
                self.logger.exception(f"Error closing NodeTaskManager: {str(e)}")
