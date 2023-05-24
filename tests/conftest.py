import pathlib
import pytest
import sqlalchemy as sa
from japier import Service


@pytest.fixture
def connection(tmp_path: pathlib.Path):
    db_path = tmp_path.joinpath('db.sqlite3')
    engine = sa.create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        yield conn


@pytest.fixture
def service(connection: sa.Connection):
    service = Service(
        coll_cfgs=[
            {
                "name": "project",
                "fields": [
                    {
                        "name": "name",
                        "type": "text"
                    }
                ]
            },
            {
                "name": "request",
                "fields": [
                    {
                        "name": "project_id",
                        "type": "ref",
                        "ref": "project"
                    },
                    {
                        "name": "stakeholders",
                        "type": "collection",
                        "fields": [
                            {
                                "name": "email",
                                "type": "text"
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
    project_input = {
        'name': 'test-name'
    }
    project_input_2 = {
        'name': 'test-name-2'
    }
    project_output = service.insert('project', project_input)
    request_input = {
        'project_id': project_output['id'],
        'stakeholders': [
            {
                'email': 'email1'
            },
            {
                'email': 'email2'
            }
        ]
    }
    request_input_2 = {
        'project_id': project_output['id'],
        'stakeholders': [
            {
                'email': 'email3'
            },
            {
                'email': 'email4'
            },
            {
                'email': 'email5'
            }
        ]
    }
    request_output = service.insert('request', request_input)
    return {
        "project": {
            "in": project_input,
            "in_2": project_input_2,
            "out": project_output
        },
        "request": {
            "in": request_input,
            "in_2": request_input_2,
            "out": request_output
        }
    }
