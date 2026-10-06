import logging
from pathlib import Path


# =========================================================
# Project Root and Log Directory
# =========================================================

PHASE2_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PHASE2_DIR.parent

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

APPLICATION_LOG = LOG_DIR / "application.log"
ERROR_LOG = LOG_DIR / "errors.log"


# =========================================================
# Logger Factory
# =========================================================

def get_logger(name):
    """Return a project-wide configured logger."""

    logger = logging.getLogger(name)

    if not logger.handlers:

        logger.setLevel(logging.INFO)
        logger.propagate = False

        # -------------------------------------------------
        # Log format
        # -------------------------------------------------

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        # -------------------------------------------------
        # Application log
        # -------------------------------------------------

        application_handler = logging.FileHandler(
            APPLICATION_LOG,
            mode="a",
            encoding="utf-8"
        )

        application_handler.setLevel(logging.INFO)
        application_handler.setFormatter(formatter)

        # -------------------------------------------------
        # Error log
        # -------------------------------------------------

        error_handler = logging.FileHandler(
            ERROR_LOG,
            mode="a",
            encoding="utf-8"
        )

        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)

        # -------------------------------------------------
        # Attach handlers
        # -------------------------------------------------

        logger.addHandler(application_handler)
        logger.addHandler(error_handler)

    return logger