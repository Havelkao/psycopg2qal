import psycopg2
from functools import wraps


def handle_db_errors(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)

        except (Exception, psycopg2.Error) as error:
            self.conn.rollback()
            print("[ERROR]", error)
            # raise error

    return wrapper
