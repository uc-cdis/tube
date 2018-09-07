import itertools
import json
import os
from operator import itemgetter

import psycopg2
import pytest
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from psycopg2.extensions import AsIs
from psycopg2.extras import RealDictCursor
from psycopg2.sql import SQL, Identifier, Literal

import tube.settings as config
from tube.spark.parsing.parser import Parser
from tube.utils import get_edge_table, get_node_table_name

test_data_folder = "./tests/test_data"

test_files = os.listdir(test_data_folder)
# remove two "metadata" files
test_files.remove("NodeDescriptions.json")
test_files.remove("DataImportOrder.txt")
# filter out dot-files
test_files = filter(lambda x: x[0] != ".", test_files)
test_files = map(lambda x: os.path.splitext(x)[0], test_files)


def items_in_file(filename):
    in_json = ".".join([filename, "json"])
    in_json_path = os.path.join(test_data_folder, in_json)

    with open(in_json_path, "r") as f:
        entries = json.load(f)
        entries = sorted(entries, key=itemgetter("submitter_id"))
    total_entries = len(entries)

    return total_entries, entries


@pytest.mark.parametrize("filename", test_files)
def test_total_number_equal_db(filename):
    total_in_files, _ = items_in_file(filename)

    conn = psycopg2.connect(config.PYDBC)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    table = AsIs("node_{table}".format(table=filename.replace("_", "")))

    cur.execute("SELECT COUNT(*) FROM %s;", (table,))
    total_subjects_in_db = cur.fetchall()[0]["count"]

    assert total_in_files == total_subjects_in_db


@pytest.mark.parametrize("filename", test_files)
def test_items_equal_db(filename):
    _, entries = items_in_file(filename)

    conn = psycopg2.connect(config.PYDBC)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    table = AsIs("node_{table}".format(table=filename.replace("_", "")))

    cur.execute("SELECT _props FROM %s;", (table,))
    prop_json = cur.fetchall()
    prop_json = map(lambda item: item["_props"], prop_json)
    prop_json = sorted(prop_json, key=itemgetter("submitter_id"))

    for x, y in zip(entries, prop_json):
        keys_x = set(x.keys())
        keys_y = set(y.keys())

        keys_xy = keys_x.intersection(keys_y)

        assert len(keys_xy) != 0

        for key in keys_xy:
            assert x[key] == y[key]


def test_ensure_input_files_total():
    input_files = os.listdir(test_data_folder)
    assert len(input_files) == 53


@pytest.fixture
def total_subjects_in_file():
    in_subject_json = os.path.join(test_data_folder, "subject.json")

    with open(in_subject_json, "r") as f:
        subjects = json.load(f)
    total_subjects_in_file = len(subjects)

    return total_subjects_in_file


def test_subject_number_equal_file_es(total_subjects_in_file):
    es = Elasticsearch([{"host": config.ES["es.nodes"], "port": config.ES["es.port"]}])
    s = Search(using=es, index="etl")
    query = s.query()
    total_subjects_in_es = query.count()

    assert total_subjects_in_file == total_subjects_in_es


@pytest.fixture
def init_parser():
    p = Parser(config.MAPPING_FILE, config.DICTIONARY_URL)
    return p


def pairwise(iterable):
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = itertools.tee(iterable)
    next(b, None)
    return itertools.izip(a, b)


def get_table_list_from_path(p, root, path):
    r = []
    if path != "":
        splitted_path = path.split(".")
    else:
        splitted_path = []

    node = get_node_table_name(p.models, root)
    r.append(node)

    for i in splitted_path:
        root, node = get_edge_table(p.models, root, i)
        r.append(node)

        node = get_node_table_name(p.models, root)
        r.append(node)
    return r


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


def execute_sql_query(sql):
    conn = psycopg2.connect(config.PYDBC)
    cur = conn.cursor()
    cur.execute(sql)
    val = cur.fetchone()
    return val[0] if val else None


def get_path_by_name(p, name):
    mapping = p.mapping

    root = mapping.keys()[0]

    for k, v in mapping[root].items():
        if k == "_aggregated_props":
            filtered = filter(lambda i: name == i["name"], v)
            if filtered:
                return filtered[0]

        if k == "_flatten_props":
            filtered = filter(lambda i: name in i["_props"], v)
            if filtered:
                filtered[0]["fn"] = "_get"
                return filtered[0]
            
        if k == "_props":
            filtered = filter(lambda i: name == i, v)
            if filtered:
                return {"path": "", "fn": "_get"}
    return None


def get_from_elasticsearch():
    es = Elasticsearch([{"host": config.ES["es.nodes"], "port": config.ES["es.port"]}])
    s = Search(using=es, index="etl")
    total = s.count()
    s = s[0:total]
    results = s.execute()
    return results


def order_table_list(table_list):
    table_list.reverse()
    return table_list


# @pytest.mark.parametrize("path", paths)
def test_get_list_from_path(init_parser):
    p = init_parser

    results = get_from_elasticsearch()

    for i in results:
        for j in i:
            name = j
            value = i[j]
            print("name: {}".format(name))
            print("value: {}".format(value))

            path = get_path_by_name(p, name)
            if not path:
                continue
            fn = path["fn"]

            table_list = get_table_list_from_path(p, "subject", path["path"])
            table_list_in_order = order_table_list(table_list)

            sql = generate_sql_from_table_list(table_list_in_order, fn, name, i.submitter_id)
            val = execute_sql_query(sql)

            print("db value: {}".format(val))

            assert i.__getattr__(name) == val
