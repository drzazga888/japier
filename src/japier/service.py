from typing import Type, Any, cast, Optional
import sqlalchemy as sa
import marshmallow as ma
from .fields import Field, DEFAULT_FIELDS
from .db import DBService


class Service:

    schemas: dict[str, Type[ma.Schema]]
    tables: dict[str, dict]
    coll_cfgs: dict[str, dict]

    def __init__(
            self,
            coll_cfgs: list[dict],
            fields: Optional[dict[str, Type[Field]]] = None
    ) -> None:
        self.fields = DEFAULT_FIELDS.copy()
        if fields:
            self.fields.update(fields)
        self.coll_cfgs = {c['name']: c for c in coll_cfgs}
        self.metadata = sa.MetaData()
        self.schemas = {
            c['name']: self._schema_from_collection(c, parent_colls=[])
            for c in coll_cfgs
        }
        self.tables = {
            c['name']: self._tables_from_collection(c, parent_colls=[])
            for c in coll_cfgs
        }

    def deserialize(self, coll_name: str, data: Any) -> dict:
        schema = self.schemas[coll_name]()
        return cast(dict, schema.load(data))
    
    def serialize(self, coll_name: str, data: dict) -> dict:
        schema = self.schemas[coll_name]()
        return cast(dict, schema.dump(data))

    def serialize_many(self, coll_name: str, data: list[Any]) -> list[dict]:
        schema = self.schemas[coll_name]()
        return cast(list[dict], schema.dump(data, many=True))
    
    def db(self, connection: sa.Connection) -> DBService:
        return DBService(self.schemas, self.tables, connection)

    def _tables_from_collection(self, coll_cfg: dict, parent_colls: list[dict]) -> dict:
        parent_name = '_'.join(c['name'] for c in parent_colls)
        name = f"{parent_name}_{coll_cfg['name']}" if parent_name else coll_cfg['name']
        cols = [
            self._get_field({"name": "id", "type": "id"}).get_sqlalchemy_column()
        ]
        children = {}
        if parent_colls:
            last_parent_cfg = parent_colls[-1]
            cols.append(
                self._get_field({
                    "name": f"{last_parent_cfg['name']}_id",
                    "type": "ref",
                    "ref_path": (c['name'] for c in parent_colls),
                    "cascade_on_delete": True
                }).get_sqlalchemy_column()
            )
        for field_cfg in coll_cfg['fields']:
            if field_cfg['type'] == 'collection':
                children[field_cfg['name']] = self._tables_from_collection(field_cfg, parent_colls + [coll_cfg])
            else:
                cols.append(
                    self._get_field(field_cfg).get_sqlalchemy_column()
                )
        table = sa.Table(name, self.metadata, *cols)
        return {
            "table": table,
            "children": children
        }

    def _schema_from_collection(self, coll_cfg: dict, parent_colls: list[dict]) -> Type[ma.Schema]:
        name = ''.join(f"{c['name']}_" for c in parent_colls) + coll_cfg['name']
        fields: dict[str, ma.fields.Field | type] = {
            "id": self._get_field({"name": "id", "type": "id"}).get_marshmallow_field()
        }
        for field_cfg in coll_cfg['fields']:
            if field_cfg['type'] == 'collection':
                fields[field_cfg['name']] = ma.fields.List(
                    ma.fields.Nested(self._schema_from_collection(field_cfg, parent_colls + [coll_cfg])),
                    required=True
                )
            else:
                fields[field_cfg['name']] = self._get_field(field_cfg).get_marshmallow_field()
        return ma.Schema.from_dict(
            fields=fields,
            name=name
        )

    def _get_field(self, field_cfg: dict) -> Field:
        field_cls = self.fields.get(field_cfg['type'])
        if field_cls:
            return field_cls(field_cfg)
        raise Exception(f'Unsupported field type: {field_cfg["type"]}')
