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


def test_select(client: FlaskClient, seed: list[dict]):
    for seed_item in seed:
        res = client.get(f"/{seed_item['name']}/{seed_item['out']['id']}")
        assert res.json == seed_item['out']
        assert res.status_code == 200


def test_insert(client: FlaskClient, seed: list[dict]):
    for seed_item in seed:
        res = client.post(f"/{seed_item['name']}", json=seed_item['in'])
        out_no_id = dict(res.get_json())
        id_ = out_no_id.pop('id')
        assert out_no_id == seed_item['in']
        assert isinstance(id_, int)
        assert res.status_code == 201


def test_update(client: FlaskClient, seed: list[dict]):
    for seed_item in seed:
        res = client.put(f"/{seed_item['name']}/{seed_item['out']['id']}", json=seed_item['in_2'])
        out_no_id = dict(res.get_json())
        id_ = out_no_id.pop('id')
        assert out_no_id == seed_item['in_2']
        assert isinstance(id_, int)
        assert res.status_code == 200


def test_delete(client: FlaskClient, seed: list[dict]):
    for seed_item in reversed(seed):
        res = client.delete(f"/{seed_item['name']}/{seed_item['out']['id']}")
        assert res.json is None
        assert res.status_code == 204
