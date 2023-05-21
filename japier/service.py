from typing import Type, Any, cast, Optional
from sqlalchemy import MetaData, Table, Column, Integer, Identity, CursorResult, Connection, ForeignKey
from marshmallow import Schema, fields
from .fields import Field, DEFAULT_FIELDS


class Service:

    schemas: dict[str, Type[Schema]]
    tables: dict[str, Table]
    coll_cfgs: dict[str, dict]

    def __init__(
            self,
            coll_cfgs: list[dict],
            connection: Connection,
            fields: Optional[dict[str, Type[Field]]] = None
    ) -> None:
        self.fields = DEFAULT_FIELDS.copy()
        if fields:
            self.fields.update(fields)
        self.coll_cfgs = {c['name']: c for c in coll_cfgs}
        self.connection = connection
        self.metadata = MetaData()
        self.schemas = {
            c['name']: self._schema_from_collection(c, parent_cfgs=[])
            for c in coll_cfgs
        }
        self.tables = self._tables_from_collections(coll_cfgs, parent_cfgs=[])

    def push_tables(self) -> None:
        self.metadata.drop_all(self.connection, checkfirst=True)
        self.metadata.create_all(self.connection, checkfirst=False)

    def deserialize(self, coll_name: str, data: Any) -> dict:
        schema = self.schemas[coll_name]()
        return cast(dict, schema.load(data))

    def serialize(self, coll_name: str, data: dict) -> dict:
        schema = self.schemas[coll_name]()
        return cast(dict, schema.dump(data))

    def select(self, coll_name: str, id_: int) -> Optional[dict]:
        table = self.tables[coll_name]
        stmt = table.select().where(table.c.id == id_)
        result = cast(CursorResult, self.connection.execute(stmt))
        row = result.first()
        if row:
            schema = self.schemas[coll_name]()
            return cast(dict, schema.dump(row._asdict()))
        return None

    def insert(self, coll_name: str, data: dict) -> dict:
        return self._insert_impl(self.coll_cfgs[coll_name], data, parent_cfgs=[])
    
    def _insert_impl(self, coll_cfg: dict, data: dict, parent_cfgs: list[dict]) -> dict:
        data_children = {}
        for child_coll in coll_cfg.get('children', []):
            data_children[child_coll['name']] = data.pop(child_coll['name'])
        name_prefix = ''.join(f"{c['name']}_" for c in parent_cfgs)
        stmt = self.tables[name_prefix + coll_cfg['name']].insert().values(**data)
        cursor_result = cast(CursorResult, self.connection.execute(stmt))
        if cursor_result and cursor_result.inserted_primary_key:
            result = {
                **data,
                "id": cursor_result.inserted_primary_key[0]
            }
            for child_coll in coll_cfg.get('children', []):
                result[child_coll['name']] = [
                    self._insert_impl(
                        child_coll,
                        {**d, f"{coll_cfg['name']}_id": result['id']},
                        parent_cfgs + [coll_cfg]
                    )
                    for d in data_children[child_coll['name']]
                ]
            return result
        raise Exception('ID cannot be retrieved')

    def update(self, coll_name: str, id_: int, data: dict) -> None:
        table = self.tables[coll_name]
        stmt = table.update().values(**data).where(table.c.id == id_)
        self.connection.execute(stmt)

    def delete(self, coll_name: str, id_: int) -> None:
        table = self.tables[coll_name]
        stmt = table.delete().where(table.c.id == id_)
        self.connection.execute(stmt)

    def _tables_from_collections(self, coll_cfgs: list[dict], parent_cfgs: list[dict]) -> dict[str, Table]:
        tables: dict[str, Table] = {}
        name_prefix = ''.join(f"{c['name']}_" for c in parent_cfgs)
        for coll_cfg in coll_cfgs:
            name = name_prefix + coll_cfg['name']
            cols = [
                Column('id', Integer, Identity(), primary_key=True)
            ]
            if parent_cfgs:
                last_parent_cfg = parent_cfgs[-1]
                cols.append(
                    Column(
                        f"{last_parent_cfg['name']}_id",
                        Integer,
                        ForeignKey(f"{last_parent_cfg['name']}.id", ondelete='CASCADE'),
                        nullable=False
                    )
                )
            cols.extend(
                self._get_field(f).get_sqlalchemy_column()
                for f in coll_cfg['fields']
            )
            tables[name] = Table(
                name,
                self.metadata,
                *cols
            )
            child_colls = coll_cfg.get('children', [])
            if child_colls:
                tables.update(self._tables_from_collections(child_colls, parent_cfgs + [coll_cfg])
            )
        return tables

    def _schema_from_collection(self, coll_cfg: dict, parent_cfgs: list[dict]) -> Type[Schema]:
        name = ''.join(f"{c['name']}_" for c in parent_cfgs) + coll_cfg['name']
        schema_fields: dict[str, fields.Field | type] = {
            "id": fields.Integer(dump_only=True)
        }
        schema_fields.update({
            f['name']: self._get_field(f).get_marshmallow_field()
            for f in coll_cfg['fields']
        })
        for child_coll in coll_cfg.get('children', []):
            schema_fields[child_coll['name']] = fields.List(
                fields.Nested(self._schema_from_collection(child_coll, parent_cfgs + [coll_cfg])),
                required=True
            )
        return Schema.from_dict(
            fields=schema_fields,
            name=name
        )

    def _get_field(self, field_cfg: dict) -> Field:
        field_cls = self.fields.get(field_cfg['type'])
        if field_cls:
            return field_cls(field_cfg)
        raise Exception(f'Unsupported field type: {field_cfg["type"]}')
