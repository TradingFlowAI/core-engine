"""服务模块"""


def setup_services(app):
    """设置应用服务"""
    from tradingflow.py_worker.services.health_check import setup_health_check

    setup_health_check(app)
