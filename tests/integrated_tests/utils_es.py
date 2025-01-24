from itertools import chain

from elasticsearch import Elasticsearch

import tube.settings as config


def get_es_connection():
    return Elasticsearch([config.ES_CONNECTION_CONFIG])


def get_item_from_elasticsearch(index, doc_type, item):
    es = get_es_connection()
    search_results = es.search(
        index=index, body={"query": {"match": {"submitter_id": item}}}, size=9999
    )
    print("Response:")
    print(search_results)
    result = search_results["hits"]["hits"]
    total = len(result)
    hits = result[0:total]
    # results = s.execute()
    return [h.get("_source") for h in hits]


def get_names(p):
    mapping = p.mapping
    names = []

    for k, v in list(mapping.items()):
        if k == "aggregated_props":
            names.extend([{"name": i["name"]} for i in v])

        if k == "flatten_props":
            names.extend(chain(*[i["props"] for i in v]))

        if k == "props":
            names.extend(v)
    res = [v["name"] for v in names]
    print(res)
    return res


def get_doc_types(interpreter):
    return [dt.doc_type for dt in interpreter.translators]
