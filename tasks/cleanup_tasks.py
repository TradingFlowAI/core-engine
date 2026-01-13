"""
Cleanup Tasks: 定期清理过期数据

提供清理功能：
1. 清理过期的 Signal 记录
2. 清理过期的暂停状态
3. 清理过期的执行日志

可以作为定时任务运行，也可以手动触发。
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# 🔥 默认配置
DEFAULT_SIGNAL_RETENTION_DAYS = 7
DEFAULT_LOG_RETENTION_DAYS = 30
DEFAULT_PAUSE_MAX_AGE_SECONDS = 604800  # 7 天


async def cleanup_expired_signals(
    retention_days: int = DEFAULT_SIGNAL_RETENTION_DAYS,
) -> Dict[str, Any]:
    """
    清理过期的 Signal 记录。
    
    Args:
        retention_days: 保留天数
        
    Returns:
        清理结果
    """
    try:
        from infra.db.services.flow_execution_signal_service import (
            FlowExecutionSignalService,
        )
        
        service = FlowExecutionSignalService()
        result = await service.cleanup_expired_signals(retention_days=retention_days)
        
        logger.info(
            f"[cleanup_expired_signals] Completed: "
            f"deleted={result.get('deleted_count', 0)}, "
            f"retention_days={retention_days}"
        )
        
        return result
        
    except Exception as e:
        logger.exception(f"[cleanup_expired_signals] Error: {e}")
        return {
            'deleted_count': 0,
            'success': False,
            'error': str(e),
        }


async def cleanup_expired_logs(
    retention_days: int = DEFAULT_LOG_RETENTION_DAYS,
) -> Dict[str, Any]:
    """
    清理过期的执行日志。
    
    Args:
        retention_days: 保留天数
        
    Returns:
        清理结果
    """
    try:
        from infra.db.services.flow_execution_log_service import (
            FlowExecutionLogService,
        )
        
        service = FlowExecutionLogService()
        
        # 检查是否有 cleanup 方法
        if hasattr(service, 'cleanup_old_logs'):
            result = await service.cleanup_old_logs(retention_days=retention_days)
        else:
            # 如果没有专门的清理方法，记录日志
            logger.info(
                f"[cleanup_expired_logs] FlowExecutionLogService does not have "
                f"cleanup_old_logs method, skipping"
            )
            result = {
                'deleted_count': 0,
                'success': True,
                'message': 'cleanup method not available',
            }
        
        logger.info(
            f"[cleanup_expired_logs] Completed: "
            f"deleted={result.get('deleted_count', 0)}"
        )
        
        return result
        
    except Exception as e:
        logger.exception(f"[cleanup_expired_logs] Error: {e}")
        return {
            'deleted_count': 0,
            'success': False,
            'error': str(e),
        }


async def cleanup_expired_pause_states(
    max_age_seconds: int = DEFAULT_PAUSE_MAX_AGE_SECONDS,
) -> Dict[str, Any]:
    """
    清理过期的暂停状态。
    
    Args:
        max_age_seconds: 最大存活时间（秒）
        
    Returns:
        清理结果
    """
    try:
        from flow.pause_manager import get_pause_manager
        
        manager = get_pause_manager()
        await manager.initialize()
        
        # 1. 清理已过期的引用
        ref_result = await manager.cleanup_expired_pause_states()
        
        # 2. 清理过时的暂停状态
        stale_result = await manager.cleanup_stale_pause_states(
            max_age_seconds=max_age_seconds
        )
        
        total_cleaned = (
            ref_result.get('cleaned_count', 0) + 
            stale_result.get('cleaned_count', 0)
        )
        
        logger.info(
            f"[cleanup_expired_pause_states] Completed: "
            f"cleaned={total_cleaned}, "
            f"max_age_seconds={max_age_seconds}"
        )
        
        return {
            'cleaned_refs': ref_result.get('cleaned_count', 0),
            'cleaned_stale': stale_result.get('cleaned_count', 0),
            'total_cleaned': total_cleaned,
            'success': True,
        }
        
    except Exception as e:
        logger.exception(f"[cleanup_expired_pause_states] Error: {e}")
        return {
            'total_cleaned': 0,
            'success': False,
            'error': str(e),
        }


async def cleanup_partial_run_data(
    max_age_hours: int = 24,
) -> Dict[str, Any]:
    """
    清理过期的 partial run 执行数据。
    
    Args:
        max_age_hours: 最大存活时间（小时）
        
    Returns:
        清理结果
    """
    try:
        import redis.asyncio as aioredis
        from infra.config import CONFIG
        
        redis_url = CONFIG.get("REDIS_URL", "redis://localhost:6379/0")
        redis_client = await aioredis.from_url(redis_url, decode_responses=True)
        
        cleaned = 0
        pattern = "partial_run:*"
        
        async for key in redis_client.scan_iter(match=pattern):
            # 检查 TTL，如果已经设置了 TTL 则跳过
            ttl = await redis_client.ttl(key)
            if ttl == -1:  # 没有设置过期时间
                # 设置一个默认的过期时间
                await redis_client.expire(key, max_age_hours * 3600)
                cleaned += 1
        
        await redis_client.close()
        
        logger.info(
            f"[cleanup_partial_run_data] Completed: "
            f"updated_ttl={cleaned}"
        )
        
        return {
            'updated_ttl': cleaned,
            'success': True,
        }
        
    except Exception as e:
        logger.exception(f"[cleanup_partial_run_data] Error: {e}")
        return {
            'updated_ttl': 0,
            'success': False,
            'error': str(e),
        }


async def run_all_cleanup_tasks(
    signal_retention_days: int = DEFAULT_SIGNAL_RETENTION_DAYS,
    log_retention_days: int = DEFAULT_LOG_RETENTION_DAYS,
    pause_max_age_seconds: int = DEFAULT_PAUSE_MAX_AGE_SECONDS,
) -> Dict[str, Any]:
    """
    运行所有清理任务。
    
    Args:
        signal_retention_days: Signal 保留天数
        log_retention_days: 日志保留天数
        pause_max_age_seconds: 暂停状态最大存活时间
        
    Returns:
        所有清理任务的结果
    """
    start_time = datetime.now(timezone.utc)
    
    logger.info("[run_all_cleanup_tasks] Starting all cleanup tasks...")
    
    results = {}
    
    # 1. 清理过期 Signal
    results['signals'] = await cleanup_expired_signals(
        retention_days=signal_retention_days
    )
    
    # 2. 清理过期日志
    results['logs'] = await cleanup_expired_logs(
        retention_days=log_retention_days
    )
    
    # 3. 清理过期暂停状态
    results['pause_states'] = await cleanup_expired_pause_states(
        max_age_seconds=pause_max_age_seconds
    )
    
    # 4. 清理 partial run 数据
    results['partial_runs'] = await cleanup_partial_run_data()
    
    end_time = datetime.now(timezone.utc)
    duration = (end_time - start_time).total_seconds()
    
    results['summary'] = {
        'started_at': start_time.isoformat(),
        'completed_at': end_time.isoformat(),
        'duration_seconds': duration,
        'all_success': all(
            r.get('success', False) for r in results.values() 
            if isinstance(r, dict) and 'success' in r
        ),
    }
    
    logger.info(
        f"[run_all_cleanup_tasks] Completed in {duration:.2f}s, "
        f"success={results['summary']['all_success']}"
    )
    
    return results


# ============================================================================
# CLI 入口点
# ============================================================================

async def main():
    """命令行入口点"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Cleanup expired data')
    parser.add_argument(
        '--signal-days', type=int, default=DEFAULT_SIGNAL_RETENTION_DAYS,
        help=f'Signal retention days (default: {DEFAULT_SIGNAL_RETENTION_DAYS})'
    )
    parser.add_argument(
        '--log-days', type=int, default=DEFAULT_LOG_RETENTION_DAYS,
        help=f'Log retention days (default: {DEFAULT_LOG_RETENTION_DAYS})'
    )
    parser.add_argument(
        '--pause-seconds', type=int, default=DEFAULT_PAUSE_MAX_AGE_SECONDS,
        help=f'Pause state max age seconds (default: {DEFAULT_PAUSE_MAX_AGE_SECONDS})'
    )
    parser.add_argument(
        '--only', choices=['signals', 'logs', 'pause', 'partial'],
        help='Run only specific cleanup task'
    )
    
    args = parser.parse_args()
    
    if args.only:
        if args.only == 'signals':
            result = await cleanup_expired_signals(retention_days=args.signal_days)
        elif args.only == 'logs':
            result = await cleanup_expired_logs(retention_days=args.log_days)
        elif args.only == 'pause':
            result = await cleanup_expired_pause_states(max_age_seconds=args.pause_seconds)
        elif args.only == 'partial':
            result = await cleanup_partial_run_data()
        print(f"Result: {result}")
    else:
        result = await run_all_cleanup_tasks(
            signal_retention_days=args.signal_days,
            log_retention_days=args.log_days,
            pause_max_age_seconds=args.pause_seconds,
        )
        print(f"Results: {result}")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
