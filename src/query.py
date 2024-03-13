import pandas as pd
from psycopg2.extensions import connection
from psycopg2.sql import SQL
from psycopg2.extras import execute_values, NamedTupleCursor
from .utils import handle_db_errors
from .query_builder import QueryBuilder


class Query(QueryBuilder):
    """Base class that adds convenience methods for CRUD operations."""

    def __init__(self, conn: connection, table: str):
        self.table = table
        self.conn = conn
        self.cursor = self.conn.cursor(cursor_factory=NamedTupleCursor)

    @handle_db_errors
    def execute(
        self, commit: bool = False, debug: bool = False
    ) -> pd.DataFrame | list | None:
        operation = self.operation or self.template.split(" ")[0]
        query = self.get_query()

        if debug:
            print(self.get_query_string())

        match operation:
            case "SELECT":
                self.cursor.execute(query, self.placeholder_values)
                data = self.cursor.fetchall()

                if self.to_pandas and len(data) > 0:
                    data = pd.DataFrame(data, columns=data[0]._fields)

                return [] if data is None else data

            case "GET":
                self.cursor.execute(query, self.placeholder_values)
                datum = self.cursor.fetchone()
                return datum

            case "INSERT" | "UPDATE":
                query = self.get_query()
                self.cursor.execute(query, self.placeholder_values)
                if commit is True:
                    self.conn.commit()
                if self.returning:
                    return self.cursor.fetchone().id

            case "DELETE":
                self.cursor.execute(query, self.placeholder_values)

            case "INSERT_DF":
                execute_values(self.cursor, query, self.placeholder_values)

            case _:
                raise "[ERROR] No operation specified"

        if commit is True:
            self.conn.commit()
            return True

    def get_query(self):
        return SQL(self.template).format(*self.query_params)

    def get_query_string(self):
        """Note: placeholder values are shown without quotes"""
        return self.get_query().as_string(self.conn) % tuple(self.placeholder_values)
