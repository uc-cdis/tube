import pytest
from elasticsearch import Elasticsearch
from elasticsearch import client
from elasticsearch_dsl import Search

import tube.settings as config
from tests.utils import items_in_file
from tests.utils_db import execute_sql_query
from tests.utils_es import get_subject_from_elasticsearch, get_path_by_name, get_table_list_from_path, order_table_list, \
    get_names
from tests.utils_sql import generate_sql_from_table_list
from tube.spark.parsing.parser import Parser


def test_auth_resource_path_exist():
    """
    Check that the field "auth_resource_path" exist
    """
    es = Elasticsearch([{"host": config.ES["es.nodes"], "port": config.ES['es.port']}])
    s = Search(using=es, index=config.ES["es.resource"])
    response = s.execute()

    auth_resource_path = "/programs/ndh/projects/test"

    for hit in response:
        assert hit.auth_resource_path == auth_resource_path


def test_es_types():
    """
    Check that no field have "text" type
    """
    es = Elasticsearch([{"host": config.ES["es.nodes"], "port": config.ES['es.port']}])

    indices = client.IndicesClient(es)

    mapping = indices.get_mapping(index=config.ES["es.resource"])

    for k, t in mapping[config.ES["es.resource"]]["mappings"]["subject"]["properties"].items():
        assert t["type"] != "text"


@pytest.fixture
def init_parser():
    p = Parser(config.MAPPING_FILE, config.DICTIONARY_URL)
    return p


@pytest.mark.parametrize("subject", [v["submitter_id"] for v in items_in_file("subject")[2]])
@pytest.mark.parametrize("name", get_names(init_parser()))
def test_get_list_from_path(init_parser, subject, name):
    p = init_parser

    results = get_subject_from_elasticsearch(subject)

    assert len(results) == 1

    result = results[0]

    value = result.__getattr__(name) if name in result else None

    path = get_path_by_name(p, name)

    fn = path["fn"]

    table_list = get_table_list_from_path(p, "subject", path["path"])
    table_list_in_order = order_table_list(table_list)

    sql = generate_sql_from_table_list(table_list_in_order, fn, name, subject)
    val = execute_sql_query(sql)

    assert value == val
