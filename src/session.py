import os
import psycopg2
from psycopg2.extensions import connection


class Session:
    def __init__(self, dsn=None, debug=False):
        self.dsn = dsn
        self.debug = debug

    def __del__(self):
        self.close()

    def connect(self) -> connection:
        dsn = self.dsn or os.environ.get("DB_URL")

        try:
            conn = psycopg2.connect(dsn)
            if self.debug:
                print("Established connection to the database")
            self.conn = conn
            return self.conn

        except psycopg2.Error as error:
            raise error

    def close(self):
        if self.debug:
            print("Closing connection to database")
        if hasattr(self, "conn") and not self.conn.closed:
            self.conn.close()
        if hasattr(self, "cursor") and not self.cursor.closed:
            self.cursor.close()
