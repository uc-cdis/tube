from contextlib import contextmanager

from psycopg2.extras import DictCursor
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.sql import SQL, Identifier, Literal

import tube.settings as config
from tests.utils import pairwise

pools = {"test_db": ThreadedConnectionPool(1, 20, dsn=config.PYDBC, connect_timeout=30)}


@contextmanager
def get_db_connection(db):
    pool = pools[db]
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


def execute_sql_query(sql):
    with get_db_cursor("test_db") as cur:
        cur.execute(sql)
        val = cur.fetchall()
        return val[0][0] if val else None


class SQLQuery:
    def __init__(self):
        self.select_clauses = {}
        self.join_clauses = {}

    def __getitem__(self, item):
        tables, fn, name, submitter_id = item
        tables = self.order_tables(tables)

        if (tuple(tables), fn) in self.join_clauses:
            sql_join = self.join_clauses[(tuple(tables), fn)]
        else:
            query = "FROM {joins}"
            joins = self.generate_join_clauses(tables)
            sql_join = SQL(query).format(joins=SQL(" ").join(joins))

            self.join_clauses[(tuple(tables), fn, name)] = sql_join

        if name in self.select_clauses:
            sql_select = self.select_clauses[name]
        else:
            query = "SELECT {gather}"
            gather = self.generate_gather_clause(fn, tables[0], name)
            sql_select = SQL(query).format(gather=gather)

            self.select_clauses[name] = sql_select

        submitter_json = '{{"submitter_id": "{}"}}'.format(submitter_id)
        sql_where = SQL("WHERE {last_node}.{_props} @> {submitter};").format(last_node=Identifier(tables[-1]),
                                                                             _props=Identifier("_props"),
                                                                             submitter=Literal(submitter_json))

        sql = SQL(" ").join([sql_select, sql_join, sql_where])
        val = execute_sql_query(sql)

        return val

    @staticmethod
    def generate_join_clauses(tables):
        """

        :param tables:
        :return:
        """
        root = tables[0]
        joins = [Identifier(root)]
        left_is_node = True

        # iterate over pairs either node + edge or edge + node
        for p1, p2 in pairwise(tables):
            # if left table is the node table than join on 'src_id' column in the edge table
            # otherwise, left table is the edge table and join on 'dst_id' column in the edge table
            if left_is_node:
                join_query = "JOIN {p2} ON ({p1}.{node_id} = {p2}.{col})"
                col = Identifier("src_id")
            else:
                join_query = "JOIN {p2} ON ({p1}.{col} = {p2}.{node_id})"
                col = Identifier("dst_id")

            add = SQL(join_query).format(p1=Identifier(p1),
                                         p2=Identifier(p2),
                                         node_id=Identifier("node_id"),
                                         col=col)

            joins.append(add)
            left_is_node = not left_is_node

        return joins

    @staticmethod
    def generate_gather_clause(aggregator, table=None, prop=None):
        """

        :param aggregator:
        :param table:
        :param prop:
        :return:
        """
        if aggregator == "_get":
            select = SQL("{table}.{column} -> {field}").format(table=Identifier(table),
                                                               column=Identifier("_props"),
                                                               field=Literal(prop))
        elif aggregator == "count":
            select = SQL("COUNT(*)")
        else:
            select = SQL("*")
        return select

    @staticmethod
    def order_tables(tables):
        """
        It will actually **return** reversed list.
        Not very useful, but more concise usage.

        :param tables:
        :return:
        """
        tables.reverse()
        return tables
