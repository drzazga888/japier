import pathlib
from collections import OrderedDict
import pytest
import sqlalchemy as sa
import sqlalchemy.event as sa_event
from japier import Service


def _fk_pragma_on_connect(dbapi_con, con_record):
    dbapi_con.execute('pragma foreign_keys=ON')


@pytest.fixture
def connection(tmp_path: pathlib.Path):
    db_path = tmp_path.joinpath('db.sqlite3')
    engine = sa.create_engine(f"sqlite:///{db_path}")
    sa_event.listen(engine, 'connect', _fk_pragma_on_connect)
    with engine.connect() as conn:
        yield conn


@pytest.fixture
def service(connection: sa.Connection):
    service = Service(
        coll_cfgs=[
            {
                "name": "category",
                "fields": [
                    {
                        "name": "name",
                        "type": "text"
                    }
                ]
            },
            {
                "name": "computer",
                "fields": [
                    {
                        "name": "category_id",
                        "type": "ref",
                        "ref_path": ("category",),
                        "cascade_on_delete": False
                    },
                    {
                        "name": "disks",
                        "type": "collection",
                        "fields": [
                            {
                                "name": "partitions",
                                "type": "collection",
                                "fields": [
                                    {
                                        "name": "name",
                                        "type": "text"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ],
        connection=connection
    )
    service.metadata.create_all(connection)
    return service


@pytest.fixture
def seed(service: Service):
    category_input = {
        'name': 'test-name'
    }
    category_input_2 = {
        'name': 'test-name-2'
    }
    category_output = service.insert('category', category_input)
    computer_input = {
        'category_id': category_output['id'],
        'disks': [
            {
                'partitions': [
                    {
                        "name": "system"
                    },
                    {
                        "name": "data1"
                    }
                ]
            },
            {
                'partitions': [
                    {
                        "name": "recovery"
                    },
                    {
                        "name": "data2"
                    }
                ]
            }
        ]
    }
    computer_input_2 = {
        'category_id': category_output['id'],
        'disks': [
            {
                'partitions': [
                    {
                        "name": "data"
                    }
                ]
            },
            {
                'partitions': [
                    {
                        "name": "system"
                    },
                    {
                        "name": "recovery"
                    }
                ]
            }
        ]
    }
    computer_output = service.insert('computer', computer_input)
    return [
        {
            "name": "category",
            "in": category_input,
            "in_2": category_input_2,
            "out": category_output
        },
        {
            "name": "computer",
            "in": computer_input,
            "in_2": computer_input_2,
            "out": computer_output
        }
    ]
