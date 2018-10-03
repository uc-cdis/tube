import pytest
from elasticsearch import Elasticsearch
from elasticsearch import client
from elasticsearch_dsl import Search

import tube.settings as config
from tests.utils import items_in_file
from tests.utils_es import get_names
from tests.value.aggregator_value import AggregatorValue
from tests.value.es_value import ESValue
from tests.value.value import value_diff
from tube.spark.indexers.interpreter import Interpreter


@pytest.fixture
def init_interpreter():
    interpreter = Interpreter(None, None, config)
    return interpreter


@pytest.fixture
def init_translator(doc_type):
    interpreter = Interpreter(None, None, config)
    for translator in interpreter.translators:
        if translator.parser.doc_type == doc_type:
            return translator


@pytest.mark.parametrize("doc_type", [dt.parser.doc_type for dt in init_interpreter().translators.values()])
def test_auth_resource_path_exist(init_interpreter, doc_type):
    """
    Check that the field "auth_resource_path" exist
    """
    interpreter = init_interpreter
    parser = interpreter.translators[doc_type].parser
    es = Elasticsearch([{"host": config.ES["es.nodes"], "port": config.ES['es.port']}])
    s = Search(using=es, index=parser.name, doc_type=doc_type)
    response = s.execute()

    auth_resource_path = "/programs/ndh/projects/test"

    for hit in response:
        assert hit.auth_resource_path == auth_resource_path


@pytest.mark.parametrize("doc_type", [dt.parser.doc_type for dt in init_interpreter().translators.values()])
def test_es_types(init_interpreter, doc_type):
    """
    Check that no field have "text" type
    """
    interpreter = init_interpreter
    parser = interpreter.translators[doc_type].parser
    es = Elasticsearch([{"host": config.ES["es.nodes"], "port": config.ES['es.port']}])

    indices = client.IndicesClient(es)

    mapping = indices.get_mapping(index=parser.name)

    for k, t in mapping[parser.name]["mappings"][doc_type]["properties"].items():
        assert t["type"] != "text"


@pytest.mark.parametrize("doc_type", [dt.parser.doc_type for dt in init_interpreter().translators.values()])
def test_get_list_from_path(init_interpreter, doc_type):
    if doc_type == 'file':
        return
    interpreter = init_interpreter
    items = items_in_file(doc_type)[2]
    parser = interpreter.translators[doc_type].parser
    names = get_names(parser)

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

        value = AggregatorValue(parser, submitter_id, doc_type, names)

        equal, diff = value_diff(results, value)
        if not equal:
            fails.append(diff)

    assert fails == []
