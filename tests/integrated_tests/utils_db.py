from psycopg2.sql import SQL, Identifier, Literal

from tests.integrated_tests.utils import pairwise
from tube.utils.db import execute_sql_query_return_first_field


class SQLQuery:
    def __init__(self):
        self.select_clauses = {}
        self.join_clauses = {}

    def __getitem__(self, item):
        tables, fn, name, src, submitter_id = item
        tables = self.order_tables(tables)

        if (tuple(tables), fn, name) in self.join_clauses:
            sql_join = self.join_clauses[(tuple(tables), fn, name)]
        else:
            query = "FROM {joins}"
            joins = self.generate_join_clauses(tables)
            sql_join = SQL(query).format(joins=SQL(" ").join(joins))

            self.join_clauses[(tuple(tables), fn, name)] = sql_join

        key = "{}_{}_{}_{}_{}".format(fn, tables[0], name, src, tables[-1])
        if key in self.select_clauses:
            sql_select, group = self.select_clauses[key]
        else:
            query = "SELECT {gather}"
            gather, group = self.generate_gather_clause(
                fn, tables[0], name, src, tables[-1]
            )
            sql_select = SQL(query).format(gather=gather)

            self.select_clauses[key] = (sql_select, group)

        submitter_json = '{{"submitter_id": "{}"}}'.format(submitter_id)
        sql_where = SQL("WHERE {last_node}.{_props} @> {submitter}").format(
            last_node=Identifier(tables[-1]),
            _props=Identifier("_props"),
            submitter=Literal(submitter_json),
        )
        if group:
            sql = SQL(" ").join([sql_select, sql_join, sql_where, group, SQL(";")])
        else:
            sql = SQL(" ").join([sql_select, sql_join, sql_where, SQL(";")])
        print(f"SQL statement: {sql}")
        val = execute_sql_query_return_first_field(sql)
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

            add = SQL(join_query).format(
                p1=Identifier(p1),
                p2=Identifier(p2),
                node_id=Identifier("node_id"),
                col=col,
            )

            joins.append(add)
            left_is_node = not left_is_node

        return joins

    @staticmethod
    def generate_gather_clause(aggregator, table=None, prop=None, src=None, root=None):
        """

        :param aggregator:
        :param table:
        :param prop:
        :param src:
        :param root:
        :return:
        """
        group = None
        if aggregator == "_get":
            select = SQL("{table}.{column} -> {field}").format(
                table=Identifier(table),
                column=Identifier("_props"),
                field=Literal(prop),
            )
        elif aggregator == "count":
            select = SQL("COUNT(*)")
        elif aggregator == "set":
            select = SQL(
                "array_agg(DISTINCT {table}.{column}->{field}), {root}.{node_id}"
            ).format(
                table=Identifier(table),
                column=Identifier("_props"),
                field=Literal(src),
                root=Identifier(root),
                node_id=Identifier("node_id"),
            )
            group = SQL("GROUP BY {root}.{node_id} ORDER BY {root}.{node_id}").format(
                root=Identifier(root), node_id=Identifier("node_id")
            )
        elif aggregator == "sum":
            select = SQL("SUM(coalesce(({table}.{props}->>{field})::int, 0))").format(
                table=Identifier(table), props=Identifier("_props"), field=Literal(src)
            )
        else:
            select = SQL("*")
        return select, group

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
