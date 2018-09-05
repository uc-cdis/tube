from elasticsearch import Elasticsearch, client

import tube.settings as config


def test_es_types():
    es = Elasticsearch([{"host": config.ES["es.nodes"], "port": config.ES['es.port']}])

    indices = client.IndicesClient(es)

    mapping = indices.get_mapping(index=config.ES["es.resource"])

    for k, t in mapping[config.ES["es.resource"]]["mappings"]["subject"]["properties"].items():
        assert t["type"] != "text"
