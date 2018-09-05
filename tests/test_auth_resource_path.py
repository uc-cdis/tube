from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

import tube.settings as config


def test_auth_resource_path_exist():
    es = Elasticsearch([{"host": config.ES["es.nodes"], "port": config.ES['es.port']}])
    s = Search(using=es, index=config.ES["es.resource"])
    response = s.execute()

    auth_resource_path = "/programs/ndh/projects/test"

    for hit in response:
        assert hit.auth_resource_path == auth_resource_path
