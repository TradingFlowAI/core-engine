import json
from datetime import datetime

from redis import Redis

# Redis连接默认参数
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
TASK_EXPIRE_SECONDS = 86400  # 24小时


def get_redis_client():
    """获取Redis客户端实例"""
    return Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


def save_task_info(task_id, task_info):
    """保存任务信息到Redis"""
    redis_client = get_redis_client()
    redis_client.setex(
        f"vault_task:{task_id}", TASK_EXPIRE_SECONDS, json.dumps(task_info)
    )


def get_task_info(task_id):
    """从Redis获取任务信息"""
    redis_client = get_redis_client()
    task_info_json = redis_client.get(f"vault_task:{task_id}")
    return json.loads(task_info_json) if task_info_json else None


def update_task_status(task_id, status, result=None, error=None):
    """更新任务状态"""
    redis_client = get_redis_client()

    # 获取现有任务信息
    task_info_json = redis_client.get(f"vault_task:{task_id}")
    task_info = json.loads(task_info_json) if task_info_json else {}

    # 更新状态
    task_info["status"] = status
    task_info["updated_at"] = datetime.now().isoformat()

    if result is not None:
        task_info["result"] = result

    if error is not None:
        task_info["error"] = error

    # 保存回Redis
    redis_client.setex(
        f"vault_task:{task_id}", TASK_EXPIRE_SECONDS, json.dumps(task_info)
    )

    return task_info


def get_all_vault_tasks():
    """获取所有Vault任务"""
    redis_client = get_redis_client()
    import re

    # 获取所有vault_task前缀的键
    keys = redis_client.keys("vault_task:*")

    tasks = []
    for key in keys:
        task_id = re.sub(r"^vault_task:", "", key.decode("utf-8"))
        task_info = get_task_info(task_id)
        if task_info:
            tasks.append(
                {
                    "task_id": task_id,
                    "status": task_info.get("status", "unknown"),
                    "created_at": task_info.get("created_at"),
                    "updated_at": task_info.get("updated_at"),
                }
            )

    return tasks
