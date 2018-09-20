from itertools import chain

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

import tube.settings as config
from tube.utils import get_edge_table, get_node_table_name


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


def get_all_from_elasticsearch():
    es = Elasticsearch([{"host": config.ES["es.nodes"], "port": config.ES["es.port"]}])
    s = Search(using=es, index="etl")
    total = s.count()
    s = s[0:total]
    results = s.execute()
    return results


def get_subject_from_elasticsearch(subject):
    es = Elasticsearch([{"host": config.ES["es.nodes"], "port": config.ES["es.port"]}])
    s = Search(using=es, index="etl") \
        .query("match", submitter_id=subject)
    total = s.count()
    s = s[0:total]
    results = s.execute()
    return results


def order_table_list(table_list):
    table_list.reverse()
    return table_list


def get_names(p):
    mapping = p.mapping

    root = mapping.keys()[0]

    names = []

    for k, v in mapping[root].items():
        if k == "_aggregated_props":
            names.extend([i["name"] for i in v])

        if k == "_flatten_props":
            names.extend(chain(*[i["_props"] for i in v]))

        if k == "_props":
            names.extend(v)

    return names
