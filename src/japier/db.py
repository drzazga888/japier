from typing import Type, cast, Optional
import sqlalchemy as sa
import marshmallow as ma


class DBService:

    def __init__(
            self,
            schemas: dict[str, Type[ma.Schema]],
            tables: dict[str, dict],
            connection: sa.Connection
    ) -> None:
        self.schemas = schemas
        self.tables = tables
        self.connection = connection

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
