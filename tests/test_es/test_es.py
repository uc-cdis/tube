import pytest
from elasticsearch import Elasticsearch
from elasticsearch import client

import tube.settings as config
from tube.etl.indexers.interpreter import create_translators
from tests.utils import items_in_file
from tests.utils_db import SQLQuery
from tests.utils_es import get_names
from tests.value.aggregator_value import AggregatorValue
from tests.value.es_value import ESValue
from tests.value.value import value_diff

dict_translators = create_translators(None, config)
doc_types = [dt.parser.doc_type for dt in list(dict_translators.values())]


@pytest.mark.parametrize("doc_type", doc_types)
def test_auth_resource_path_exist(doc_type):
    """
    Check that the field "auth_resource_path" exist
    """
    parser = dict_translators[doc_type].parser
    es = Elasticsearch(
        [
            {
                "host": config.ES["es.nodes"],
                "port": int(config.ES["es.port"]),
                "scheme": "http",
            }
        ]
    )
    response = es.search(
        index=parser.name, body={"query": {"match_all": {}}}, size=9999
    )

    auth_resource_path = "/programs/jnkns/projects/jenkins"

    for hit in response:
        assert hit.auth_resource_path == auth_resource_path


@pytest.mark.parametrize("doc_type", doc_types)
def test_es_types(doc_type):
    """
    Check that no field have "text" type
    """
    parser = dict_translators[doc_type].parser
    es = Elasticsearch(
        [
            {
                "host": config.ES["es.nodes"],
                "port": int(config.ES["es.port"]),
                "scheme": "http",
            }
        ]
    )

    indices = client.IndicesClient(es)
    index_name = list(indices.get_alias(name=parser.name).keys())[0]

    mapping = indices.get_mapping(index=index_name)

    # assert "_None_id" not in mapping[index_name]["mappings"][doc_type]["properties"]
    list_errors = []
    for k, t in list(mapping[index_name]["mappings"][doc_type]["properties"].items()):
        try:
            assert t["type"] != "text", f"field {k} has type as text"
        except AssertionError as ex:
            list_errors.append(ex)
    assert list_errors == []


@pytest.mark.parametrize("doc_type", doc_types)
def test_get_list_from_path(doc_type):
    if doc_type in ["file", "project"]:
        return
    excluded_file_name = f".{doc_type}-excluded"
    items = items_in_file(doc_type)[2]
    excluded_submitted_ids = set(
        [item["submitter_id"] for item in items_in_file(excluded_file_name)[2]]
    )

    parser = dict_translators[doc_type].parser
    names = get_names(parser)

    # SQL query instance for query memoization
    sql = SQLQuery()

    fails = []
    for item in items:
        submitter_id = item["submitter_id"]
        if submitter_id in excluded_submitted_ids:
            continue
        results = ESValue(parser, submitter_id, doc_type, names)

        result_length = results.length

        if result_length != 1:
            if result_length < 1:
                fails.append(
                    "Not exist expected {doc_type} with submitter_id {item} in ES".format(
                        doc_type=doc_type, item=submitter_id
                    )
                )
            else:
                fails.append(
                    "Duplicated {doc_type} with submitter_id {item} in ES".format(
                        doc_type=doc_type, item=submitter_id
                    )
                )

        value = AggregatorValue(sql, parser, submitter_id, doc_type, names)

        equal, diff = value_diff(results, value)
        if not equal:
            fails.append(diff)

    assert fails == []
