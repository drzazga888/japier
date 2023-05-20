from typing import Type, Any, cast, Optional
from sqlalchemy import MetaData, Table, Column, Integer, Identity, CursorResult, Connection
from marshmallow import Schema, fields
from .fields import Field, DEFAULT_FIELDS


class Service:

    def __init__(
            self,
            collections: list[dict],
            connection: Connection,
            fields: Optional[dict[str, Type[Field]]] = None
    ) -> None:
        self.fields = DEFAULT_FIELDS.copy()
        if fields:
            self.fields.update(fields)
        self.collections = collections
        self.connection = connection
        self.metadata = MetaData()
        self.tables = {c['name']: self._generate_table(c) for c in collections}
        self.schemas = {c['name']: self._generate_schema(c) for c in collections}

    def push_tables(self) -> None:
        self.metadata.drop_all(self.connection, checkfirst=True)
        self.metadata.create_all(self.connection, checkfirst=False)

    def insert(self, collection: str, data_serialized: Any) -> int:
        schema = self.schemas[collection]()
        data = cast(dict, schema.load(data_serialized))
        stmt = self.tables[collection].insert().values(**data)
        result = cast(CursorResult, self.connection.execute(stmt))
        if result and result.inserted_primary_key:
            return result.inserted_primary_key[0]
        raise Exception('ID cannot be retrieved')
    
    def select(self, collection: str, id_: int) -> Optional[dict]:
        table = self.tables[collection]
        stmt = table.select().where(table.c.id == id_)
        result = cast(CursorResult, self.connection.execute(stmt))
        row = result.first()
        if row:
            schema = self.schemas[collection]()
            return cast(dict, schema.dump(row._asdict()))
        return None

    def _generate_table(self, collection: dict) -> Table:
        return Table(
            collection['name'],
            self.metadata,
            Column('id', Integer, Identity(), primary_key=True),
            *[
                self._get_field(f).get_sqlalchemy_column()
                for f in collection['fields']
            ]
        )

    def _generate_schema(self, collection: dict) -> Type[Schema]:
        return Schema.from_dict(
            fields={
                "id": fields.Integer(dump_only=True),
                **{
                    f['name']: self._get_field(f).get_marshmallow_field()
                    for f in collection['fields']
                }
            },
            name=collection['name']
        )
    
    def _get_field(self, field_cfg: dict) -> Field:
        field_cls = self.fields.get(field_cfg['type'])
        if field_cls:
            return field_cls(field_cfg)
        raise Exception(f'Unsupported field type: {field_cfg["type"]}')
