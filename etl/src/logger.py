# etl/src/logger.py

import logging
from logging.handlers import RotatingFileHandler
import os
from etl.src.config import (
    EXTRACT_METADATA_LOG,
    EXTRACT_FIXTURES_LOG,
    TRANSFORM_FIXTURES_LOG,
    LOAD_TO_DB_LOG,
    UPDATE_FIXTURES_LOG,
    LOAD_UPDATES_LOG,
)

def get_logger(name: str, log_path: str = None) -> logging.Logger:
    """
    Configure and return a logger for the given module name.

    The log file path resolution order is:
      1. Explicit `log_path` argument.
      2. Environment variable named <MODULE_NAME>_LOG.
      3. Default path from config constants mapping `name` to a log file.
      4. EXTRACT_METADATA_LOG as a final fallback.

    Logs are written to a RotatingFileHandler (5MB per file, 3 backups).
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        # Determine log file path
        if log_path:
            path = log_path
        else:
            env_key = f"{name.upper()}_LOG"
            path = os.getenv(env_key)
            if not path:
                defaults = {
                    'extract_metadata': EXTRACT_METADATA_LOG,
                    'extract_fixtures': EXTRACT_FIXTURES_LOG,
                    'transform_fixtures': TRANSFORM_FIXTURES_LOG,
                    'load_to_db': LOAD_TO_DB_LOG,
                    'update_played_fixtures': UPDATE_FIXTURES_LOG,
                    'load_updates': LOAD_UPDATES_LOG ,
                }
                path = defaults.get(name, EXTRACT_METADATA_LOG)

        # Ensure directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Create rotating file handler
        handler = RotatingFileHandler(
            filename=path,
            maxBytes=5 * 1024 * 1024,
            backupCount=3
        )
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger