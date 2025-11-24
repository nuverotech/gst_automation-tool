import logging
import sys
from pythonjsonlogger import jsonlogger
from app.config import settings


def setup_logger(name: str) -> logging.Logger:
    """
    Setup logger with JSON formatting
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper()))
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    handler = logging.StreamHandler(sys.stdout)
    
    if settings.LOG_FORMAT == "json":
        formatter = jsonlogger.JsonFormatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s'
        )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger
