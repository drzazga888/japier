from typing import Callable
from functools import partial
import sqlalchemy as sa
from flask import Flask, g, Blueprint, jsonify, request
from werkzeug.exceptions import NotFound
from .service import Service
from .db import DBService


_DB_SERVICE_KEY = 'japier_db_service'


ConnectionGetter = Callable[[], sa.Connection]


class JapierFlask:

    def __init__(self, service: Service, connection_getter: ConnectionGetter) -> None:
        self.service = service
        self.connection_getter = connection_getter

    def init_app(self, app: Flask) -> None:
        for name in self.service.coll_cfgs.keys():
            bp = Blueprint(name, __name__)
            bp.add_url_rule('', endpoint='select_many', view_func=partial(self._select_many, name), methods=['GET'])
            bp.add_url_rule('<int:id_>', endpoint='select', view_func=partial(self._select, name), methods=['GET'])
            bp.add_url_rule('', endpoint='insert', view_func=partial(self._insert, name), methods=['POST'])
            bp.add_url_rule('<int:id_>', endpoint='update', view_func=partial(self._update, name), methods=['PUT'])
            bp.add_url_rule('<int:id_>', endpoint='delete', view_func=partial(self._delete, name), methods=['DELETE'])
            app.register_blueprint(bp, url_prefix=f'/{name}')

    @property
    def db_service(self) -> DBService:
        if hasattr(g, _DB_SERVICE_KEY):
            return getattr(g, _DB_SERVICE_KEY)
        db_service = self.service.db(self.connection_getter())
        setattr(g, _DB_SERVICE_KEY, db_service)
        return db_service

    def _select_many(self, name: str):
        result = self.db_service.select_many(name)
        return jsonify(
            self.service.serialize_many(name, result)
        ), 200

    def _select(self, name: str, id_: int):
        result = self.db_service.select(name, id_)
        if not result:
            raise NotFound()
        return jsonify(
            self.service.serialize(name, result)
        ), 200

    def _insert(self, name: str):
        data = self.service.deserialize(name, request.json)
        result = self.db_service.insert(name, data)
        return jsonify(
            self.service.serialize(name, result)
        ), 201

    def _update(self, name: str, id_: int):
        data = self.service.deserialize(name, request.json)
        result = self.db_service.update(name, id_, data)
        return jsonify(
            self.service.serialize(name, result)
        ), 200
    
    def _delete(self, name: str, id_: int):
        self.db_service.delete(name, id_)
        return '', 204
