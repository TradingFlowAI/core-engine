"""Flow Execution Signal Service

Service for node-to-node signal records.
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from infra.db.base import get_db_session
from infra.db.models.flow_execution_signal import FlowExecutionSignal

logger = logging.getLogger(__name__)


class FlowExecutionSignalService:
    """Node-to-node signal record service class"""
    
    async def save_signal(
        self,
        flow_id: str,
        cycle: int,
        direction: str,
        from_node_id: Optional[str] = None,
        to_node_id: Optional[str] = None,
        source_handle: Optional[str] = None,
        target_handle: Optional[str] = None,
        signal_type: str = 'ANY',
        data_type: str = 'unknown',
        payload: Optional[Dict[str, Any]] = None,
    ) -> Optional[FlowExecutionSignal]:
        """
        Save signal record.
        
        Args:
            flow_id: Flow ID
            cycle: Execution cycle
            direction: Signal direction ('input' or 'output')
            from_node_id: Source node ID
            to_node_id: Target node ID
            source_handle: Source handle
            target_handle: Target handle
            signal_type: Signal type
            data_type: Data type
            payload: Signal data
            
        Returns:
            Saved signal record, None if failed
        """
        session = None
        try:
            session = get_db_session()
            
            signal = FlowExecutionSignal(
                flow_id=flow_id,
                cycle=cycle,
                direction=direction,
                from_node_id=from_node_id,
                to_node_id=to_node_id,
                source_handle=source_handle,
                target_handle=target_handle,
                signal_type=signal_type,
                data_type=data_type,
                payload=payload,
                created_at=datetime.utcnow(),
            )
            
            session.add(signal)
            session.commit()
            session.refresh(signal)
            
            logger.debug(
                f"Signal saved: {from_node_id}:{source_handle} -> {to_node_id}:{target_handle}"
            )
            
            return signal
            
        except Exception as e:
            logger.error(f"Failed to save signal: {e}")
            if session:
                try:
                    session.rollback()
                except Exception:
                    pass
            return None
        finally:
            if session:
                try:
                    session.close()
                except Exception:
                    pass
    
    async def get_signals_by_flow_cycle(
        self,
        flow_id: str,
        cycle: int,
        direction: Optional[str] = None,
        node_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get signal records for specified flow/cycle.
        
        Args:
            flow_id: Flow ID
            cycle: Execution cycle
            direction: Optional, filter by direction ('input' or 'output')
            node_id: Optional, filter by node ID (as from or to)
            limit: Record count limit
            
        Returns:
            List of signal records
        """
        session = None
        try:
            session = get_db_session()
            
            query = session.query(FlowExecutionSignal).filter(
                FlowExecutionSignal.flow_id == flow_id,
                FlowExecutionSignal.cycle == cycle
            )
            
            if direction:
                query = query.filter(FlowExecutionSignal.direction == direction)
            
            if node_id:
                query = query.filter(
                    (FlowExecutionSignal.from_node_id == node_id) |
                    (FlowExecutionSignal.to_node_id == node_id)
                )
            
            query = query.order_by(FlowExecutionSignal.created_at.desc())
            query = query.limit(limit)
            
            signals = query.all()
            
            return [signal.to_dict() for signal in signals]
            
        except Exception as e:
            logger.error(f"Failed to get signals: {e}")
            return []
        finally:
            if session:
                try:
                    session.close()
                except Exception:
                    pass
    
    async def get_signals_by_node(
        self,
        flow_id: str,
        cycle: int,
        node_id: str,
        limit: int = 50,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get input and output signals for specified node.
        
        Args:
            flow_id: Flow ID
            cycle: Execution cycle
            node_id: Node ID
            limit: Record count limit per direction
            
        Returns:
            {
                'input': [list of input signals],
                'output': [list of output signals]
            }
        """
        session = None
        try:
            session = get_db_session()
            
            # Get input signals (to_node_id == node_id)
            input_query = session.query(FlowExecutionSignal).filter(
                FlowExecutionSignal.flow_id == flow_id,
                FlowExecutionSignal.cycle == cycle,
                FlowExecutionSignal.to_node_id == node_id,
                FlowExecutionSignal.direction == 'input'
            ).order_by(FlowExecutionSignal.created_at.desc()).limit(limit)
            
            # Get output signals (from_node_id == node_id)
            output_query = session.query(FlowExecutionSignal).filter(
                FlowExecutionSignal.flow_id == flow_id,
                FlowExecutionSignal.cycle == cycle,
                FlowExecutionSignal.from_node_id == node_id,
                FlowExecutionSignal.direction == 'output'
            ).order_by(FlowExecutionSignal.created_at.desc()).limit(limit)
            
            input_signals = input_query.all()
            output_signals = output_query.all()
            
            return {
                'input': [s.to_dict() for s in input_signals],
                'output': [s.to_dict() for s in output_signals],
            }
            
        except Exception as e:
            logger.error(f"Failed to get node signals: {e}")
            return {'input': [], 'output': []}
        finally:
            if session:
                try:
                    session.close()
                except Exception:
                    pass
    
    async def delete_signals_by_flow(
        self,
        flow_id: str,
        before_cycle: Optional[int] = None,
    ) -> int:
        """
        Delete signal records for specified flow.
        
        Args:
            flow_id: Flow ID
            before_cycle: Optional, only delete records before this cycle
            
        Returns:
            Number of deleted records
        """
        session = None
        try:
            session = get_db_session()
            
            query = session.query(FlowExecutionSignal).filter(
                FlowExecutionSignal.flow_id == flow_id
            )
            
            if before_cycle is not None:
                query = query.filter(FlowExecutionSignal.cycle < before_cycle)
            
            count = query.delete()
            session.commit()
            
            logger.info(f"Deleted {count} signals for flow {flow_id}")
            return count
            
        except Exception as e:
            logger.error(f"Failed to delete signals: {e}")
            if session:
                try:
                    session.rollback()
                except Exception:
                    pass
            return 0
        finally:
            if session:
                try:
                    session.close()
                except Exception:
                    pass
