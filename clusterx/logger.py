# Copied from ddm
import logging
import logging.config
from logging import getLogger

logger_config = dict(
    version=1,
    disable_existing_loggers=False,
    formatters={
        "default": {
            "format": "%(message)s",
            "datefmt": "[%X]",
        }
    },
    handlers={
        "rich": {
            "class": "rich.logging.RichHandler",
            "show_path": False,
            "formatter": "default",
        },
    },
    loggers={
        "clusterx": {
            "handlers": ["rich"],
            "level": logging.INFO,
        },
    },
)

logging.config.dictConfig(logger_config)

__all__ = ["getLogger"]
