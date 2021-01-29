"""This is where we defined the Config files, which are used for initiating the
application with specific settings such as logger configurations or different
database setups."""

from app.utils.logging import file_logger, client_logger
from decouple import config as env_conf
import logging


class Production:
    SECRET_KEY = env_conf("SECRET_KEY", cast=str, default="12345")

    @staticmethod
    def init_app(app):
        # The default Flask logger level is set at ERROR, so if you want to see
        # INFO level or DEBUG level logs, you need to lower the main loggers
        # level first.
        app.logger.setLevel(logging.DEBUG)
        app.logger.handlers.clear()
        app.logger.addHandler(file_logger)
        app.logger.addHandler(client_logger)


class Testing:
    """Configuration for running the test suite"""
    
    TESTING = True
    DEBUG = True

    SECRET_KEY = env_conf("SECRET_KEY", cast=str, default="12345")

    @staticmethod
    def init_app(app):
        # The default Flask logger level is set at ERROR, so if you want to see
        # INFO level or DEBUG level logs, you need to lower the main loggers
        # level first.
        app.logger.setLevel(logging.DEBUG)
        app.logger.handlers.clear()
        app.logger.addHandler(file_logger)
        app.logger.addHandler(client_logger)


class Develop:
    """Development config geared towards docker."""

    # API configurations
    SECRET_KEY = env_conf("SECRET_KEY", cast=str, default="12345")

    @staticmethod
    def init_app(app):
        """Initiates application."""
        app.logger.setLevel(logging.DEBUG)
        app.logger.handlers.clear()
        app.logger.addHandler(client_logger)
        app.logger.addHandler(file_logger)
