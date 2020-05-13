import os

from contextlib import contextmanager
from jinja2 import Template
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker, scoped_session

from dotenv import load_dotenv

load_dotenv()

URI = "postgres://{user}:{password}@{host}:{port}/{db}".format(
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    db=os.getenv("POSTGRES_USER"),
)
postgres_engine = create_engine(URI)

PROJECT_ROOT = os.getenv("AIRFLOW_HOME", ".")
SQL_FOLDER = f"{PROJECT_ROOT}/etl/sql"


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = scoped_session(sessionmaker(bind=postgres_engine))
    try:
        yield session
        session.commit()
    except DBAPIError:
        session.rollback()
        raise
    finally:
        session.close()


def create_table(table_name):
    sql_file = f"{SQL_FOLDER}/{table_name}.sql"
    with open(sql_file) as f:
        sql = text(f.read())

    template_file = f"{SQL_FOLDER}/templates/replace.sql"
    with open(template_file) as f:
        template = Template(f.read())

    with session_scope() as db:
        query = text(template.render(sql=sql.text, table_name=table_name))
        print(query)
        db.execute(query)


def extract_log_data():
    with session_scope() as db:
        query = text(
            """
            DROP TABLE IF EXISTS logs;
            CREATE TABLE logs (raw JSON);
            """
        )
        print("Clean up old logs in table")
        print(query)
        db.execute(query)

    DATA_FOLDER = os.getenv("DATA_FOLDER", ".")
    files = os.listdir(DATA_FOLDER)
    datafiles = [f"{DATA_FOLDER}/{filename}" for filename in files]
    for datafile in datafiles:
        print(f"Loading {datafile}")
        load_datafile(datafile)


def load_datafile(datafile, table_name="logs"):
    with session_scope() as db:
        query = text("CREATE TABLE IF NOT EXISTS logs (raw JSONB);")
        print(query)
        db.execute(query)

    values = []
    with open(datafile) as f:
        values = [row for row in f.readlines()]
        values = [row.replace("'", "''") for row in values]  # escape single-quotes
        values_str = "'),('".join(values)
        insert_sql = f"INSERT INTO logs VALUES ('{values_str}');"

    with session_scope() as db:
        query = text(insert_sql)
        print(f"INSERTing {len(values)} rows into table '{table_name}'.")
        db.execute(query)
