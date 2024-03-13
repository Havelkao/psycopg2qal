import pandas as pd
from psycopg2.sql import SQL, Identifier, Placeholder


class QueryBuilder:
    """Depedent class, cannot be used on its own"""

    def init(self):
        self.operation: str = None
        self.template: str = ""
        self.query_params: list = []
        self.placeholder_values: list = []
        self.returning = False

    def insert(self, **kwargs):
        self.init()
        self.template = "INSERT INTO {} ({}) VALUES ({})"
        self.query_params.extend(
            [
                Identifier(self.table),
                SQL(",").join(map(Identifier, kwargs.keys())),
                SQL(",").join(Placeholder() * len(kwargs.values())),
            ]
        )
        self.placeholder_values = tuple(kwargs.values())
        return self

    def insert_df(self, df: pd.DataFrame):
        self.init()
        self.operation = "INSERT_DF"
        self.template = "INSERT INTO {} ({}) VALUES {}"
        self.query_params.extend(
            [
                Identifier(self.table),
                SQL(",").join(map(Identifier, df.keys())),
                Placeholder(),
            ]
        )
        self.placeholder_values = df.values
        return self

    def select(self, columns: list = None, to_pandas=False):
        self.init()
        self.to_pandas = to_pandas
        self.template = "SELECT {} FROM {}"

        if columns is not None:
            columns = SQL(",").join(map(Identifier, columns))
        else:
            columns = SQL("*")

        self.query_params.extend([columns, Identifier(self.table)])
        return self

    def get(self, id=None, **kwargs):
        if id:
            self.select().filter_by(id=id).limit(1)
        else:
            self.select().filter_by(**kwargs).limit(1)
        self.operation = "GET"  # needs to be set after select, else operation would be select
        return self

    def update(self, **kwargs):
        self.init()
        self.template = "UPDATE {} SET " + ", ".join(["{} = {}"] * len(kwargs))
        self.query_params.append(Identifier(self.table))
        for key, value in kwargs.items():
            self.query_params.extend([Identifier(key), Placeholder()])
            self.placeholder_values.append(value)
        return self

    def delete(self):
        self.init()
        self.template = "DELETE FROM {}"
        self.query_params.append(Identifier(self.table))
        return self

    def returns(self):
        self.returning = True
        self.template += " RETURNING id"
        return self

    def join(self, table, ltk, rtk):
        self.template += " JOIN {} ON {} = {}"
        self.query_params.extend([Identifier(table), Identifier(ltk), Identifier(rtk)])
        return self

    def where(self, column, operator, value):
        if operator not in ["=", ">", ">=", "<", "<="]:
            raise ValueError
        clause = self._get_clause()
        self.template += clause + " {} " + operator + " {}"
        self.query_params.extend([Identifier(column), Placeholder()])
        self.placeholder_values.append(value)
        return self

    def filter_by(self, **kwargs):
        for key, value in kwargs.items():
            clause = self._get_clause()
            self.template += clause + " {} = {}"
            self.query_params.extend([Identifier(key), Placeholder()])
            self.placeholder_values.append(value)
        return self

    def between(self, column, value1, value2):
        clause = self._get_clause()
        self.template += f" {clause}" + " {} BETWEEN {} AND {}"
        self.query_params.extend([Identifier(column), Placeholder(), Placeholder()])
        self.placeholder_values.extend([value1, value2])
        return self

    def order_by(self, column, direction="ASC"):
        if direction not in ["ASC", "DESC"]:
            raise ValueError

        self.template += " ORDER BY {}" + direction
        self.query_params.append(Identifier(column))
        return self

    def limit(self, limit: int):
        self.template += " LIMIT {}"
        self.query_params.append(Placeholder())
        self.placeholder_values.append(limit)
        return self

    def _get_clause(self):
        """
        Class expects only one where clause in the template
        """
        clause = " AND" if "WHERE" in self.template else " WHERE"
        return clause
