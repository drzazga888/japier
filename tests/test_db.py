import pytest
from japier.db import DBService


pytestmark = pytest.mark.usefixtures("init_db")


def test_insert(seed: list[dict]):
    for seed_item in seed:
        out_no_id = seed_item['out'].copy()
        id_ = out_no_id.pop('id')
        assert isinstance(id_, int)
        assert seed_item['in'] == out_no_id


def test_select(db_service: DBService, seed: list[dict]):
    for seed_item in seed:
        assert db_service.select(seed_item['name'], seed_item['out']['id']) == seed_item['out']


def test_select_many(db_service: DBService, seed: list[dict]):
    for seed_item in seed:
        assert db_service.select_many(seed_item['name']) == [seed_item['out']]


def test_delete(db_service: DBService, seed: list[dict]):
    for seed_item in reversed(seed):
        db_service.delete(seed_item['name'], seed_item['out']['id'])
        assert db_service.select(seed_item['name'], seed_item['out']['id']) is None


def test_update(db_service: DBService, seed: list[dict]):
    for seed_item in seed:
        out_2 = db_service.update(seed_item['name'], seed_item['out']['id'], seed_item['in_2'])
        assert out_2 == {**seed_item['in_2'], 'id': seed_item['out']['id']}
        assert db_service.select(seed_item['name'], seed_item['out']['id']) == out_2
