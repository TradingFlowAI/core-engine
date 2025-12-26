"""Worker registration and management API"""

from infra.config import CONFIG

# Configuration
WORKER_ID = CONFIG["WORKER_ID"]
SERVER_URL = CONFIG["SERVER_URL"]


def register_worker_routes(app):
    """Register Worker management related routes"""

    # Worker registration and deregistration are handled through service start and stop events,
    # this module does not define additional API routes
