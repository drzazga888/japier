import pytest
from sqlalchemy import create_engine, Connection
from japier import Service


@pytest.fixture
def connection():
    engine = create_engine("mysql://root:12345@localhost/apier_test")
    with engine.connect() as conn:
        yield conn


@pytest.fixture
def service(connection: Connection):
    return Service(
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
                    }
                ],
                "children": [
                    {
                        "name": "stakeholders",
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


def test_test(service: Service):
    # pprint(capzilla.tables)
    # pprint(capzilla.schemas)

    service.push_tables()

    project = service.insert('project', service.deserialize('project', {
        'name': 'dupa'
    }))
    print('project', project)
    request = service.insert('request', service.deserialize('request', {
        'project_id': project['id'],
        'stakeholders': [
            {
                'email': 'email1'
            },
            {
                'email': 'email2'
            }
        ]
    }))
    print('request', request)

    request = service.update('request', request['id'], service.deserialize('request', {
        'project_id': project['id'],
        'stakeholders': [
            {
                'email': 'email1'
            },
            {
                'email': 'email3'
            }
        ]
    }))
    print('request', request)

    # capzilla.update('project', project_id, {'name': 'dupa2'})
    # print('updated')

    # project_data = capzilla.select('project', project_id)
    # print('project_data', project_data)
    # request_data = capzilla.select('request', request_id)
    # print('request_data', request_data)

    # project_data = capzilla.select('project', project_id)
    # print('project_data', project_data)
    # request_data = capzilla.select('request', request_id)
    # print('request_data', request_data)

    # capzilla.delete('project', project_id)
    # print('deleted')

    # project_data = capzilla.select('project', project_id)
    # print('project_data', project_data)
    # request_data = capzilla.select('request', request_id)
    # print('request_data', request_data)
