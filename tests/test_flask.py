import pytest
from flask import Flask
from flask.testing import FlaskClient
import sqlalchemy as sa
from japier import Service
from japier.flask import JapierFlask


pytestmark = pytest.mark.usefixtures("init_db")


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config.update({
        'TESTING': True
    })
    return app


@pytest.fixture
def client(app: Flask):
    return app.test_client()


@pytest.fixture(autouse=True)
def japier_flask(app: Flask, service: Service, connection: sa.Connection):
    japier_flask = JapierFlask(service, lambda: connection)
    japier_flask.init_app(app)
    return japier_flask


def test_select_many(client: FlaskClient, seed: list[dict]):
    for seed_item in seed:
        res = client.get(f"/{seed_item['name']}")
        assert res.json == [seed_item['out']]
        assert res.status_code == 200
