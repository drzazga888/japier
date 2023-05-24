from typing import cast, Type
import pytest
import sqlalchemy as sa
import marshmallow as ma
from japier import Service


def test_schemas(service: Service):
    project_fields = service.schemas['project']().fields
    assert set(project_fields) == {'id', 'name'}
    assert isinstance(project_fields['id'], ma.fields.Integer)
    assert isinstance(project_fields['name'], ma.fields.String)
    request_fields = service.schemas['request']().fields
    assert set(request_fields) == {'id', 'project_id', 'stakeholders'}
    assert isinstance(request_fields['id'], ma.fields.Integer)
    assert isinstance(request_fields['project_id'], ma.fields.Integer)
    assert isinstance(request_fields['stakeholders'], ma.fields.List)
    assert isinstance(request_fields['stakeholders'].inner, ma.fields.Nested)
    request_stakeholders_fields = cast(Type[ma.Schema], request_fields['stakeholders'].inner.nested)().fields
    assert set(request_stakeholders_fields) == {'id', 'email'}
    assert isinstance(request_stakeholders_fields['id'], ma.fields.Integer)
    assert isinstance(request_stakeholders_fields['email'], ma.fields.String)


def test_tables(service: Service):
    project_table = cast(sa.Table, service.tables['project']['table'])
    assert project_table.name == 'project'
    assert set(c.name for c in project_table.c) == {'id', 'name'}
    assert isinstance(project_table.c.id.type, sa.Integer)
    assert isinstance(project_table.c.name.type, sa.Text)
    request_table = cast(sa.Table, service.tables['request']['table'])
    assert request_table.name == 'request'
    assert set(c.name for c in request_table.c) == {'id', 'project_id'}
    assert isinstance(request_table.c.id.type, sa.Integer)
    assert isinstance(request_table.c.project_id.type, sa.Integer)
    request_stakeholders_table = cast(sa.Table, service.tables['request']['children']['stakeholders']['table'])
    assert request_stakeholders_table.name == 'request_stakeholders'
    assert set(c.name for c in request_stakeholders_table.c) == {'id', 'request_id', 'email'}
    assert isinstance(request_stakeholders_table.c.id.type, sa.Integer)
    assert isinstance(request_stakeholders_table.c.request_id.type, sa.Integer)
    assert isinstance(request_stakeholders_table.c.email.type, sa.Text)


def test_insert(seed: dict):
    for in_out in seed.values():
        out_no_id = in_out['out'].copy()
        id_ = out_no_id.pop('id')
        assert isinstance(id_, int)
        assert in_out['in'] == out_no_id


def test_select(service: Service, seed: dict):
    for name, in_out in seed.items():
        assert service.select(name, in_out['out']['id']) == in_out['out']


def test_select_many(service: Service, seed: dict):
    for name, in_out in seed.items():
        assert service.select_many(name) == [in_out['out']]


def test_delete(service: Service, seed: dict):
    for name, in_out in seed.items():
        service.delete(name, in_out['out']['id'])
        assert service.select(name, in_out['out']['id']) is None


def test_update(service: Service, seed: dict):
    for name, in_out in seed.items():
        out_2 = service.update(name, in_out['out']['id'], in_out['in_2'])
        assert out_2 == {**in_out['in_2'], 'id': in_out['out']['id']}
        assert service.select(name, in_out['out']['id']) == out_2
