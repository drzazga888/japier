from typing import Type, Optional
from sqlalchemy import Column, Text, Integer, ForeignKey, Identity
from marshmallow import fields, validate


class Field:

    def __init__(self, cfg: dict):
        self.cfg = cfg

    def get_sqlalchemy_column(self) -> Column:
        raise NotImplementedError()

    def get_marshmallow_field(self) -> fields.Field:
        raise NotImplementedError()
    

class IdField(Field):

    def get_sqlalchemy_column(self) -> Column:
        return Column(self.cfg['name'], Integer, Identity(), primary_key=True)

    def get_marshmallow_field(self) -> fields.Field:
        return fields.Integer(dump_only=True)


class TextField(Field):

    def get_sqlalchemy_column(self) -> Column:
        return Column(self.cfg['name'], Text, nullable=False)

    def get_marshmallow_field(self) -> fields.Field:
        return fields.String(required=True)


class RefField(Field):

    def get_sqlalchemy_column(self) -> Column:
        return Column(
            self.cfg['name'],
            Integer,
            ForeignKey(f"{self.cfg['ref']}.id", ondelete='CASCADE'),
            nullable=False
        )

    def get_marshmallow_field(self) -> fields.Field:
        return fields.Integer(required=True, strict=True, validate=validate.Range(min=1))


DEFAULT_FIELDS: dict[str, Type[Field]] = {
    "id": IdField,
    "text": TextField,
    "ref": RefField
}
