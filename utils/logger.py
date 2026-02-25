import sys
from loguru import logger
from config.settings import Config

def setup_logger():
    logger.remove()
    logger.add(sys.stdout, level=Config.LOG_LEVEL,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        colorize=True)
    logger.add(Config.LOG_FILE, level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{line} | {message}",
        rotation="10 MB", retention="30 days")
    logger.info("Logger initialized")

setup_logger()
