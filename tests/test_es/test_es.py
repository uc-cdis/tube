import pytest
from elasticsearch import Elasticsearch
from elasticsearch import client
from elasticsearch_dsl import Search

import tube.settings as config
from tube.etl.indexers.interpreter import create_translators
from tests.utils import items_in_file
from tests.utils_db import SQLQuery
from tests.utils_es import get_names
from tests.value.aggregator_value import AggregatorValue
from tests.value.es_value import ESValue
from tests.value.value import value_diff


@pytest.fixture
def init_interpreter():
    return create_translators(None, config)


@pytest.mark.parametrize("doc_type", [dt.parser.doc_type for dt in list(init_interpreter().values())])
def test_auth_resource_path_exist(init_interpreter, doc_type):
    """
    Check that the field "auth_resource_path" exist
    """
    interpreter = init_interpreter
    parser = interpreter[doc_type].parser
    es = Elasticsearch([{"host": config.ES["es.nodes"], "port": config.ES['es.port']}])
    s = Search(using=es, index=parser.name, doc_type=doc_type)
    response = s.execute()

    auth_resource_path = "/programs/jnkns/projects/jenkins"

    for hit in response:
        assert hit.auth_resource_path == auth_resource_path


@pytest.mark.parametrize("doc_type", [dt.parser.doc_type for dt in list(init_interpreter().values())])
def test_es_types(init_interpreter, doc_type):
    """
    Check that no field have "text" type
    """
    interpreter = init_interpreter
    parser = interpreter[doc_type].parser
    es = Elasticsearch([{"host": config.ES["es.nodes"], "port": config.ES['es.port']}])

    indices = client.IndicesClient(es)
    index_name = list(indices.get_alias(name=parser.name).keys())[0]

    mapping = indices.get_mapping(index=index_name)

    for k, t in list(mapping[index_name]["mappings"][doc_type]["properties"].items()):
        assert t["type"] != "text"


@pytest.mark.parametrize("doc_type", [dt.parser.doc_type for dt in list(init_interpreter().values())])
def test_get_list_from_path(init_interpreter, doc_type):
    if doc_type == 'file':
        return
    interpreter = init_interpreter
    items = items_in_file(doc_type)[2]
    parser = interpreter[doc_type].parser
    names = get_names(parser)

    # SQL query instance for query memoization
    sql = SQLQuery()

    fails = []
    for item in items:
        submitter_id = item['submitter_id']

        results = ESValue(parser, submitter_id, doc_type, names)

        result_length = results.length

        if result_length != 1:
            if result_length < 1:
                fails.append('Not exist expected {doc_type} with submitter_id {item} in ES'
                             .format(doc_type=doc_type, item=submitter_id))
            else:
                fails.append('Duplicated {doc_type} with submitter_id {item} in ES'
                             .format(doc_type=doc_type, item=submitter_id))

        value = AggregatorValue(sql, parser, submitter_id, doc_type, names)

        equal, diff = value_diff(results, value)
        if not equal:
            fails.append(diff)

    assert fails == []
