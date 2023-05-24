from typing import Callable
from functools import partial
import sqlalchemy as sa
from flask import Flask, g, Blueprint, jsonify
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
            bp.add_url_rule('', endpoint='select_many', view_func=partial(self._select_many, name=name), methods=['GET'])
            app.register_blueprint(bp, url_prefix=f'/{name}')

    @property
    def db_service(self) -> DBService:
        if hasattr(g, _DB_SERVICE_KEY):
            return getattr(g, _DB_SERVICE_KEY)
        db_service = self.service.db(self.connection_getter())
        setattr(g, _DB_SERVICE_KEY, db_service)
        return db_service

    def _select_many(self, name):
        return jsonify(
            self.service.serialize_many(
                name,
                self.db_service.select_many(name)
            )
        ), 200
