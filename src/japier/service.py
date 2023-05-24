from typing import Type, Any, cast, Optional
import sqlalchemy as sa
import marshmallow as ma
from .fields import Field, DEFAULT_FIELDS


class Service:

    schemas: dict[str, Type[ma.Schema]]
    tables: dict[str, dict]
    coll_cfgs: dict[str, dict]

    def __init__(
            self,
            coll_cfgs: list[dict],
            connection: sa.Connection,
            fields: Optional[dict[str, Type[Field]]] = None
    ) -> None:
        self.fields = DEFAULT_FIELDS.copy()
        if fields:
            self.fields.update(fields)
        self.coll_cfgs = {c['name']: c for c in coll_cfgs}
        self.connection = connection
        self.metadata = sa.MetaData()
        self.schemas = {
            c['name']: self._schema_from_collection(c, parent_cfgs=[])
            for c in coll_cfgs
        }
        self.tables = {
            c['name']: self._tables_from_collection(c, parent_cfgs=[])
            for c in coll_cfgs
        }

    def deserialize(self, coll_name: str, data: Any) -> dict:
        schema = self.schemas[coll_name]()
        return cast(dict, schema.load(data))

    def serialize(self, coll_name: str, data: dict) -> dict:
        schema = self.schemas[coll_name]()
        return cast(dict, schema.dump(data))
    
    def select_many(self, coll_name: str) -> list[dict]:
        return self._select_impl(self.tables[coll_name], coll_name, [])

    def select(self, coll_name: str, id_: int) -> Optional[dict]:
        table_cfg = self.tables[coll_name]
        table = cast(sa.Table, table_cfg['table'])
        result = self._select_impl(table_cfg, coll_name, [table.c.id == id_])
        return result[0] if result else None
    
    def _select_impl(self, table_cfg: dict, coll_name: str, filters: list[sa.ColumnElement[bool]]) -> list[dict]:
        table = cast(sa.Table, table_cfg['table'])
        stmt = table.select().where(*filters).order_by(table.c.id)
        cursor_result = cast(sa.CursorResult, self.connection.execute(stmt))
        results = []
        for row in cursor_result.all():
            result = row._asdict()
            for child_name, child_table in cast(dict, table_cfg['children']).items():
                table = cast(sa.Table, child_table['table'])
                result[child_name] = [
                    self._without_ids(coll_name, d)
                    for d in self._select_impl(
                        child_table,
                        child_name,
                        [table.c[f"{coll_name}_id"] == result['id']]
                    )
                ]
            results.append(result)
        return results
    
    def _without_ids(self, coll_name: str, data: dict) -> dict:
        result = data.copy()
        del result['id']
        del result[f"{coll_name}_id"]
        return result

    def insert(self, coll_name: str, data: dict) -> dict:
        return {
            **data,
            "id": self._insert_impl(self.tables[coll_name], coll_name, data)
        }
    
    def _insert_impl(self, table_cfg: dict, coll_name: str, data: dict) -> int:
        to_insert = data.copy()
        data_lists = {
            child_name: to_insert.pop(child_name)
            for child_name in cast(dict, table_cfg['children']).keys()
        }
        table = cast(sa.Table, table_cfg['table'])
        stmt = table.insert().values(**to_insert)
        cursor_result = cast(sa.CursorResult, self.connection.execute(stmt))
        if not cursor_result.inserted_primary_key:
            raise Exception('ID cannot be retrieved')
        id_ = cursor_result.inserted_primary_key.id
        for child_name, child_table in cast(dict, table_cfg['children']).items():
            for d in data_lists[child_name]:
                self._insert_impl(
                    child_table,
                    child_name,
                    {**d, f"{coll_name}_id": id_}
                )
        return id_

    def update(self, coll_name: str, id_: int, data: dict) -> dict:
        table_cfg = self.tables[coll_name]
        to_update = data.copy()
        data_lists = {
            child_name: to_update.pop(child_name)
            for child_name in cast(dict, table_cfg['children']).keys()
        }
        table = cast(sa.Table, self.tables[coll_name]['table'])
        stmt = table.update().values(**to_update).where(table.c.id == id_)
        self.connection.execute(stmt)
        for child_name, child_table in cast(dict, table_cfg['children']).items():
            table = cast(sa.Table, child_table['table'])
            stmt = table.delete().where(table.c[f"{coll_name}_id"] == id_)
            self.connection.execute(stmt)
            for d in data_lists[child_name]:
                self._insert_impl(
                    child_table,
                    child_name,
                    {**d, f"{coll_name}_id": id_}
                )
        return {**data, "id": id_}

    def delete(self, coll_name: str, id_: int) -> None:
        table = cast(sa.Table, self.tables[coll_name]['table'])
        stmt = table.delete().where(table.c.id == id_)
        self.connection.execute(stmt)

    def _tables_from_collection(self, coll_cfg: dict, parent_cfgs: list[dict]) -> dict:
        name = ''.join(f"{c['name']}_" for c in parent_cfgs) + coll_cfg['name']
        cols = [
            self._get_field({"name": "id", "type": "id"}).get_sqlalchemy_column()
        ]
        children = {}
        if parent_cfgs:
            last_parent_cfg = parent_cfgs[-1]
            cols.append(
                self._get_field({
                    "name": f"{last_parent_cfg['name']}_id",
                    "type": "ref",
                    "ref": last_parent_cfg['name']
                }).get_sqlalchemy_column()
            )
        for field_cfg in coll_cfg['fields']:
            if field_cfg['type'] == 'collection':
                children[field_cfg['name']] = self._tables_from_collection(field_cfg, parent_cfgs + [coll_cfg])
            else:
                cols.append(
                    self._get_field(field_cfg).get_sqlalchemy_column()
                )
        table = sa.Table(name, self.metadata, *cols)
        return {
            "table": table,
            "children": children
        }

    def _schema_from_collection(self, coll_cfg: dict, parent_cfgs: list[dict]) -> Type[ma.Schema]:
        name = ''.join(f"{c['name']}_" for c in parent_cfgs) + coll_cfg['name']
        fields: dict[str, ma.fields.Field | type] = {
            "id": self._get_field({"name": "id", "type": "id"}).get_marshmallow_field()
        }
        for field_cfg in coll_cfg['fields']:
            if field_cfg['type'] == 'collection':
                fields[field_cfg['name']] = ma.fields.List(
                    ma.fields.Nested(self._schema_from_collection(field_cfg, parent_cfgs + [coll_cfg])),
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
