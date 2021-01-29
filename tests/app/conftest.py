import pytest
from app import create_app


class TestingConfig:
    SECRET_KEY = '12345'

    @staticmethod
    def init_app(app):
        pass


@pytest.fixture
def app():
    conf = TestingConfig()
    app = create_app(conf)

    yield app
