"""Flow Execution Signal Service

Service for node-to-node signal records.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from infra.db.base import get_db_session
from infra.db.models.flow_execution_signal import FlowExecutionSignal

logger = logging.getLogger(__name__)

# 🔥 Signal 过期配置
DEFAULT_SIGNAL_RETENTION_DAYS = 7  # 默认保留 7 天
DEFAULT_MAX_SIGNALS_PER_FLOW = 1000  # 每个 Flow 最多保留 1000 条 Signal


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

    # =========================================================================
    # 🔥 Signal 过期清理方法
    # =========================================================================

    async def cleanup_expired_signals(
        self,
        retention_days: int = DEFAULT_SIGNAL_RETENTION_DAYS,
    ) -> Dict[str, Any]:
        """
        清理过期的 Signal 记录（全局清理）。
        
        Args:
            retention_days: 保留天数，超过这个天数的 Signal 将被删除
            
        Returns:
            {
                'deleted_count': int,
                'cutoff_date': str,
                'success': bool
            }
        """
        session = None
        cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
        
        try:
            session = get_db_session()
            
            # 删除过期的 Signal
            count = session.query(FlowExecutionSignal).filter(
                FlowExecutionSignal.created_at < cutoff_date
            ).delete()
            
            session.commit()
            
            logger.info(
                f"Cleaned up {count} expired signals (older than {retention_days} days)"
            )
            
            return {
                'deleted_count': count,
                'cutoff_date': cutoff_date.isoformat(),
                'retention_days': retention_days,
                'success': True,
            }
            
        except Exception as e:
            logger.error(f"Failed to cleanup expired signals: {e}")
            if session:
                try:
                    session.rollback()
                except Exception:
                    pass
            return {
                'deleted_count': 0,
                'cutoff_date': cutoff_date.isoformat(),
                'retention_days': retention_days,
                'success': False,
                'error': str(e),
            }
        finally:
            if session:
                try:
                    session.close()
                except Exception:
                    pass

    async def cleanup_signals_by_flow(
        self,
        flow_id: str,
        retention_days: Optional[int] = None,
        max_signals: int = DEFAULT_MAX_SIGNALS_PER_FLOW,
    ) -> Dict[str, Any]:
        """
        清理指定 Flow 的过期 Signal 记录。
        
        保留策略：
        1. 保留最近 retention_days 天的 Signal
        2. 保留最近 max_signals 条 Signal（如果超过限制）
        
        Args:
            flow_id: Flow ID
            retention_days: 保留天数（可选，None 表示不按时间清理）
            max_signals: 最大保留条数
            
        Returns:
            清理结果
        """
        session = None
        deleted_by_time = 0
        deleted_by_count = 0
        
        try:
            session = get_db_session()
            
            # 1. 按时间清理
            if retention_days is not None:
                cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
                deleted_by_time = session.query(FlowExecutionSignal).filter(
                    FlowExecutionSignal.flow_id == flow_id,
                    FlowExecutionSignal.created_at < cutoff_date
                ).delete()
            
            # 2. 按数量清理（保留最新的 max_signals 条）
            total_count = session.query(FlowExecutionSignal).filter(
                FlowExecutionSignal.flow_id == flow_id
            ).count()
            
            if total_count > max_signals:
                # 获取第 max_signals 条记录的 created_at
                cutoff_signal = session.query(FlowExecutionSignal).filter(
                    FlowExecutionSignal.flow_id == flow_id
                ).order_by(
                    FlowExecutionSignal.created_at.desc()
                ).offset(max_signals - 1).first()
                
                if cutoff_signal:
                    deleted_by_count = session.query(FlowExecutionSignal).filter(
                        FlowExecutionSignal.flow_id == flow_id,
                        FlowExecutionSignal.created_at < cutoff_signal.created_at
                    ).delete()
            
            session.commit()
            
            total_deleted = deleted_by_time + deleted_by_count
            if total_deleted > 0:
                logger.info(
                    f"Cleaned up {total_deleted} signals for flow {flow_id} "
                    f"(by_time={deleted_by_time}, by_count={deleted_by_count})"
                )
            
            return {
                'flow_id': flow_id,
                'deleted_by_time': deleted_by_time,
                'deleted_by_count': deleted_by_count,
                'total_deleted': total_deleted,
                'success': True,
            }
            
        except Exception as e:
            logger.error(f"Failed to cleanup signals for flow {flow_id}: {e}")
            if session:
                try:
                    session.rollback()
                except Exception:
                    pass
            return {
                'flow_id': flow_id,
                'deleted_by_time': 0,
                'deleted_by_count': 0,
                'total_deleted': 0,
                'success': False,
                'error': str(e),
            }
        finally:
            if session:
                try:
                    session.close()
                except Exception:
                    pass

    async def get_signal_stats(self, flow_id: Optional[str] = None) -> Dict[str, Any]:
        """
        获取 Signal 统计信息。
        
        Args:
            flow_id: 可选，指定 Flow ID
            
        Returns:
            统计信息
        """
        session = None
        try:
            session = get_db_session()
            
            if flow_id:
                # 指定 Flow 的统计
                total = session.query(FlowExecutionSignal).filter(
                    FlowExecutionSignal.flow_id == flow_id
                ).count()
                
                oldest = session.query(FlowExecutionSignal).filter(
                    FlowExecutionSignal.flow_id == flow_id
                ).order_by(FlowExecutionSignal.created_at.asc()).first()
                
                newest = session.query(FlowExecutionSignal).filter(
                    FlowExecutionSignal.flow_id == flow_id
                ).order_by(FlowExecutionSignal.created_at.desc()).first()
                
                return {
                    'flow_id': flow_id,
                    'total_signals': total,
                    'oldest_signal': oldest.created_at.isoformat() if oldest else None,
                    'newest_signal': newest.created_at.isoformat() if newest else None,
                }
            else:
                # 全局统计
                from sqlalchemy import func
                
                total = session.query(FlowExecutionSignal).count()
                flow_count = session.query(
                    func.count(func.distinct(FlowExecutionSignal.flow_id))
                ).scalar()
                
                oldest = session.query(FlowExecutionSignal).order_by(
                    FlowExecutionSignal.created_at.asc()
                ).first()
                
                return {
                    'total_signals': total,
                    'total_flows': flow_count,
                    'oldest_signal': oldest.created_at.isoformat() if oldest else None,
                }
                
        except Exception as e:
            logger.error(f"Failed to get signal stats: {e}")
            return {'error': str(e)}
        finally:
            if session:
                try:
                    session.close()
                except Exception:
                    pass
