from typing import cast, Type
import pytest
import sqlalchemy as sa
import marshmallow as ma
from japier import Service


def test_schemas(service: Service):
    categories_fields = service.schemas['categories']().fields
    assert set(categories_fields) == {'id', 'name'}
    assert isinstance(categories_fields['id'], ma.fields.Integer)
    assert isinstance(categories_fields['name'], ma.fields.String)
    computers_fields = service.schemas['computers']().fields
    assert set(computers_fields) == {'id', 'category_id', 'disks'}
    assert isinstance(computers_fields['id'], ma.fields.Integer)
    assert isinstance(computers_fields['category_id'], ma.fields.Integer)
    assert isinstance(computers_fields['disks'], ma.fields.List)
    assert isinstance(computers_fields['disks'].inner, ma.fields.Nested)
    disks_fields = cast(Type[ma.Schema], computers_fields['disks'].inner.nested)().fields
    assert set(disks_fields) == {'id', 'partitions'}
    assert isinstance(disks_fields['id'], ma.fields.Integer)
    assert isinstance(disks_fields['partitions'], ma.fields.List)
    assert isinstance(disks_fields['partitions'].inner, ma.fields.Nested)
    partitions_fields = cast(Type[ma.Schema], disks_fields['partitions'].inner.nested)().fields
    assert set(partitions_fields) == {'id', 'name'}
    assert isinstance(partitions_fields['id'], ma.fields.Integer)
    assert isinstance(partitions_fields['name'], ma.fields.String)


def test_tables(service: Service):
    categories_table = cast(sa.Table, service.tables['categories']['table'])
    assert categories_table.name == 'categories'
    assert {c.name for c in categories_table.c} == {'id', 'name'}
    assert isinstance(categories_table.c.id.type, sa.Integer)
    assert isinstance(categories_table.c.name.type, sa.Text)
    computers_cfg = service.tables['computers']
    computers_table = cast(sa.Table, computers_cfg['table'])
    assert computers_table.name == 'computers'
    assert {c.name for c in computers_table.c} == {'id', 'category_id'}
    assert isinstance(computers_table.c.id.type, sa.Integer)
    assert isinstance(computers_table.c.category_id.type, sa.Integer)
    disks_cfg = computers_cfg['children']['disks']
    disks_table = cast(sa.Table, disks_cfg['table'])
    assert disks_table.name == 'computers_disks'
    assert {c.name for c in disks_table.c} == {'id', 'computers_id'}
    assert isinstance(disks_table.c.id.type, sa.Integer)
    assert isinstance(disks_table.c.computers_id.type, sa.Integer)
    partitions_cfg = disks_cfg['children']['partitions']
    partitions_table = cast(sa.Table, partitions_cfg['table'])
    assert partitions_table.name == 'computers_disks_partitions'
    assert {c.name for c in partitions_table.c} == {'id', 'disks_id', 'name'}
    assert isinstance(partitions_table.c.id.type, sa.Integer)
    assert isinstance(partitions_table.c.disks_id.type, sa.Integer)
    assert isinstance(partitions_table.c.name.type, sa.Text)


@pytest.mark.usefixtures('init_db')
def test_deserialize(service: Service, seed: list[dict]):
    for seed_item in seed:
        assert service.deserialize(seed_item['name'], seed_item['in']) == seed_item['in']


@pytest.mark.usefixtures('init_db')
def test_serialize(service: Service, seed: list[dict]):
    for seed_item in seed:
        assert service.serialize(seed_item['name'], seed_item['out']) == seed_item['out']


@pytest.mark.usefixtures('init_db')
def test_serialize_many(service: Service, seed: list[dict]):
    for seed_item in seed:
        assert service.serialize_many(seed_item['name'], [seed_item['out']]) == [seed_item['out']]
