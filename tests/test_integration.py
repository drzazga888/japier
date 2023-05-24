from typing import cast, Type
import sqlalchemy as sa
import marshmallow as ma
from japier import Service


def test_schemas(service: Service):
    category_fields = service.schemas['category']().fields
    assert set(category_fields) == {'id', 'name'}
    assert isinstance(category_fields['id'], ma.fields.Integer)
    assert isinstance(category_fields['name'], ma.fields.String)
    computer_fields = service.schemas['computer']().fields
    assert set(computer_fields) == {'id', 'category_id', 'disks'}
    assert isinstance(computer_fields['id'], ma.fields.Integer)
    assert isinstance(computer_fields['category_id'], ma.fields.Integer)
    assert isinstance(computer_fields['disks'], ma.fields.List)
    assert isinstance(computer_fields['disks'].inner, ma.fields.Nested)
    disks_fields = cast(Type[ma.Schema], computer_fields['disks'].inner.nested)().fields
    assert set(disks_fields) == {'id', 'partitions'}
    assert isinstance(disks_fields['id'], ma.fields.Integer)
    assert isinstance(disks_fields['partitions'], ma.fields.List)
    assert isinstance(disks_fields['partitions'].inner, ma.fields.Nested)
    partitions_fields = cast(Type[ma.Schema], disks_fields['partitions'].inner.nested)().fields
    assert set(partitions_fields) == {'id', 'name'}
    assert isinstance(partitions_fields['id'], ma.fields.Integer)
    assert isinstance(partitions_fields['name'], ma.fields.String)


def test_tables(service: Service):
    category_table = cast(sa.Table, service.tables['category']['table'])
    assert category_table.name == 'category'
    assert {c.name for c in category_table.c} == {'id', 'name'}
    assert isinstance(category_table.c.id.type, sa.Integer)
    assert isinstance(category_table.c.name.type, sa.Text)
    computer_cfg = service.tables['computer']
    computer_table = cast(sa.Table, computer_cfg['table'])
    assert computer_table.name == 'computer'
    assert {c.name for c in computer_table.c} == {'id', 'category_id'}
    assert isinstance(computer_table.c.id.type, sa.Integer)
    assert isinstance(computer_table.c.category_id.type, sa.Integer)
    disks_cfg = computer_cfg['children']['disks']
    disks_table = cast(sa.Table, disks_cfg['table'])
    assert disks_table.name == 'computer_disks'
    assert {c.name for c in disks_table.c} == {'id', 'computer_id'}
    assert isinstance(disks_table.c.id.type, sa.Integer)
    assert isinstance(disks_table.c.computer_id.type, sa.Integer)
    partitions_cfg = disks_cfg['children']['partitions']
    partitions_table = cast(sa.Table, partitions_cfg['table'])
    assert partitions_table.name == 'computer_disks_partitions'
    assert {c.name for c in partitions_table.c} == {'id', 'disks_id', 'name'}
    assert isinstance(partitions_table.c.id.type, sa.Integer)
    assert isinstance(partitions_table.c.disks_id.type, sa.Integer)
    assert isinstance(partitions_table.c.name.type, sa.Text)


def test_insert(seed: list[dict]):
    for seed_item in seed:
        out_no_id = seed_item['out'].copy()
        id_ = out_no_id.pop('id')
        assert isinstance(id_, int)
        assert seed_item['in'] == out_no_id


def test_select(service: Service, seed: list[dict]):
    for seed_item in seed:
        assert service.select(seed_item['name'], seed_item['out']['id']) == seed_item['out']


def test_select_many(service: Service, seed: list[dict]):
    for seed_item in seed:
        assert service.select_many(seed_item['name']) == [seed_item['out']]


def test_delete(service: Service, seed: list[dict]):
    for seed_item in reversed(seed):
        service.delete(seed_item['name'], seed_item['out']['id'])
        assert service.select(seed_item['name'], seed_item['out']['id']) is None


def test_update(service: Service, seed: list[dict]):
    for seed_item in seed:
        out_2 = service.update(seed_item['name'], seed_item['out']['id'], seed_item['in_2'])
        assert out_2 == {**seed_item['in_2'], 'id': seed_item['out']['id']}
        assert service.select(seed_item['name'], seed_item['out']['id']) == out_2
