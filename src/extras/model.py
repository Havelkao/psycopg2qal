from ..query import Query
from psycopg2.extras import NamedTupleCursor


class classproperty(object):
    def __init__(self, f):
        self.f = f

    def __get__(self, _, owner):
        return self.f(owner)


class Model(Query):
    session = None

    def __init__(self, **kwargs):
        self.table = self.__tablename__
        self.__dict__.update(kwargs)
        self._insert = super().insert
        if not Model.session:
            raise "Session is not set"

        self.session = Model.session

    def insert(self):
        args = {k: v for k, v in vars(self).items() if k in self.columns}
        self._insert(**args)
        return self

    @classproperty
    def columns(cls):
        return [k for k, _ in cls.__dict__.items() if not k.startswith("_")]

    @property
    def conn(self):
        if self.has_open_conn():
            return self.session.conn

        return self.session.connect()

    @property
    def cursor(self):
        #  Query is not initialized, therefore will call cursor before establishing connection
        if not self.has_open_conn():
            self.conn

        if hasattr(self.session, "cursor") and not self.session.cursor.closed:
            return self.session.cursor
        self.session.cursor = self.session.conn.cursor(cursor_factory=NamedTupleCursor)
        return self.session.cursor

    def has_open_conn(self):
        return hasattr(self.session, "conn") and not self.session.conn.closed


class Column:
    # types TBD
    pass
