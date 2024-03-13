# psycopg2 query abstraction layer

miniature orm for basic crud operations in postgres

## Examples

```py
# no model
session = Session()
conn = session.connect()
q = Query(conn, "some_table")

q.select().execute() # get all
q.get(id=id).execute() # get one by id
q.get(col="mango", col2=42).execute() # get one by filter
q.insert(col=1, col2=2).execute(commit=True) # insert record
q.delete().filter_by(id=id).execute(commit=True) # delete record
q.update(col='henlo', col3=3).filter_by(id=id).execute(commit=True) # update record
```

```py
# model
class Instrument(Model):
    __tablename__ = "instrument"
    id = Column
    ticker = Column
    precision = Column

class Column:
    # not implemented, TBD
    pass
```

```py
# get all instruments
Instrument().select().execute()

# get insturment by id
Instrument().select().filter_by(id=id).execute()
Instrument().get(id=id).execute()

# insert an instrument
new = Instrument(ticker=ticker, precision=precision)
new.id = new.insert().returns().execute(commit=True)

# delete an insturment
Instrument().delete().filter_by(id=id).execute(commit=True)

# update an instrument
updated = (
    Instrument()
    .update(ticker=ticker, precision=precision)
    .filter_by(id=id)
    .returns()
    .execute(commit=True)
)
```

## Flask extension

```py
# extensions.db
from psycopg2qal import Session, Model
from flask import Flask


def init_app(self: Session, app: Flask):
    """
    Sets session to be used by class "Model"
    Existing connection is closed when request is finished.
    """
    Model.session = self

    @app.teardown_request
    def teardown_request(e=None):
        if Model.session is not None:
            self.close()


Session.init_app = init_app
```

```py
# extensions.__init__
from .db import Session
db = Session()
```

```py
# app.__init__
from .extensions import db

def create_app():
    ...
    return app

def register_extensions(app: Flask):
    db.init_app(app)
```

```py
# main.py
from app import create_app

app = create_app()

```

## Views

generic model for aggregated views, extra join example

```py

class TSView(Model):
    datetime = Column
    instrument_id = Column
    bid = Column
    ask = Column

    def __init__(self, agg, ticker):
        self.__tablename__ = f"timeseries_{agg}"
        super().__init__()
        self.ticker = ticker
        self._select = super().select

    def select(self, columns: list = None, to_pandas=False):
        self.to_pandas = to_pandas
        self._select(columns=columns).join(
            "instrument", "id", "instrument_id"
        ).filter_by(ticker=self.ticker)

        return self
```
