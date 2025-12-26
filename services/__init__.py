"""Services Module"""


def setup_services(app):
    """Setup application services."""
    from services.health_check import setup_health_check

    setup_health_check(app)
