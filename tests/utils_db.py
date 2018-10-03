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
    def __init__(self, tables, aggregator, prop, submitter_id):
        """

        :param tables:
        :param aggregator:
        :param prop:
        :param submitter_id:
        """
        self.tables = self.order_tables(tables)
        self.aggregator = aggregator
        self.property = prop
        self.submitter_id = submitter_id
        self.sql = self.generate_sql_from_tables()
        self.val = execute_sql_query(self.sql)

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

    def generate_sql_from_tables(self):
        """

        :return:
        """
        joins = self.generate_join_clauses(self.tables)
        gather = self.generate_gather_clause(self.aggregator, self.tables[0], self.property)
        query = "SELECT {gather} FROM {joins} WHERE {last_node}.{_props} @> {submitter};"
        submitter_json = '{{"submitter_id": "{}"}}'.format(self.submitter_id)
        sql = SQL(query).format(gather=gather,
                                joins=SQL(" ").join(joins),
                                last_node=Identifier(self.tables[-1]),
                                _props=Identifier("_props"),
                                submitter=Literal(submitter_json))
        return sql
