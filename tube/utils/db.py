from contextlib import contextmanager

from psycopg2.extras import DictCursor
from psycopg2.pool import ThreadedConnectionPool

import tube.settings as config


connection_name = "db"
pools = {}


def create_default_db_connection():
    pools[connection_name] = ThreadedConnectionPool(
        1, 20, dsn=config.PYDBC, connect_timeout=30
    )
    return pools[connection_name]


@contextmanager
def get_db_connection(db):
    pool = pools.get(db, create_default_db_connection())
    try:
        connection = pool.getconn()
        yield connection
    finally:
        pool.putconn(connection)


@contextmanager
def get_db_cursor(db, commit=False):
    with get_db_connection(db) as connection:
        cursor = connection.cursor(cursor_factory=DictCursor)
        try:
            yield cursor
            if commit:
                connection.commit()
        finally:
            cursor.close()


def execute_sql_query_return_first_field(sql):
    with get_db_cursor(connection_name) as cur:
        cur.execute(sql)
        val = cur.fetchall()
        return val[0][0] if val else None


def execute_sql_query_return_first_item(sql):
    with get_db_cursor(connection_name) as cur:
        cur.execute(sql)
        val = cur.fetchall()
        return val[0] if val else None
