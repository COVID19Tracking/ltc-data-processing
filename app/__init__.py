"""This is where we create a function for initiating the application, which is
later on used at the very top level stories.py module to initiate the
application with a specific config file"""

# Flask Imports
from flask import Flask


def create_app(config):
    app = Flask(__name__)

    app.config.from_object(config)
    config.init_app(app)

    # Register our API blueprint
    from app.api import api as api_blueprint
    app.register_blueprint(api_blueprint, url_prefix='/api')

    # register an error handler to return full exceptions for server errors
    @app.errorhandler(500)
    def internal_server_error(e):
        return str(e.original_exception), 500

    return app
