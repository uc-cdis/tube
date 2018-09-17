from psycopg2.sql import SQL, Identifier, Literal

from tests.utils import pairwise


def generate_join_clauses(table_list):
    root = table_list[0]
    joins = [Identifier(root)]
    src = True

    for p1, p2 in pairwise(table_list):
        if src:
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
        src = not src

    return joins


def generate_gather_clause(fn, table=None, prop=None):
    if fn == "_get":
        select = SQL("{}.{} -> {}").format(Identifier(table), Identifier("_props"), Literal(prop))
    elif fn == "count":
        select = SQL("COUNT(*)")
    else:
        select = SQL("*")
    return select


def generate_sql_from_table_list(table_list, fn, prop, submitter_id):
    joins = generate_join_clauses(table_list)
    gather = generate_gather_clause(fn, table_list[0], prop)
    query = "SELECT {gather} FROM {joins} WHERE {last_node}.{_props} @> {submitter};"
    submitter_json = '{{"submitter_id": "{}"}}'.format(submitter_id)
    sql = SQL(query).format(gather=gather,
                            joins=SQL(" ").join(joins),
                            last_node=Identifier(table_list[-1]),
                            _props=Identifier("_props"),
                            submitter=Literal(submitter_json))
    return sql
