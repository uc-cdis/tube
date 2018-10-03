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

    node = get_node_table_name(p.model, root)
    r.append(node)

    for i in splitted_path:
        root, node = get_edge_table(p.model, root, i)
        r.append(node)

        node = get_node_table_name(p.model, root)
        r.append(node)
    return r


def convert_mappings(value, value_mappings):
    for v in value_mappings:
        if value == v.final:
            return v.original
    return value


def get_path_by_name(p, name):
    mapping = p.mapping

    for k, v in mapping.items():
        if k == "aggregated_props":
            filtered = filter(lambda i: name == i["name"], v)
            if filtered:
                return filtered[0], None

        if k == "flatten_props":
            filtered = filter(lambda i: name in [j['name'] for j in i["props"]], v)
            value_mapping = filter(lambda x: name == x.name, chain(*(x.props for x in p.flatten_props)))
            if filtered:
                filtered[0]["fn"] = "_get"
                return filtered[0], value_mapping[0]

        if k == "props":
            filtered = filter(lambda i: name == i['name'], v)
            value_mapping = filter(lambda x: name == x.name, p.props)
            if filtered:
                return {"path": "", "fn": "_get"}, value_mapping[0]
    return None


def get_item_from_elasticsearch(index, doc_type, item):
    es = Elasticsearch([{"host": config.ES["es.nodes"], "port": config.ES["es.port"]}])
    s = Search(using=es, index=index, doc_type=doc_type) \
        .query("match", submitter_id=item)
    total = s.count()
    s = s[0:total]
    results = s.execute()
    return results


def order_table_list(table_list):
    table_list.reverse()
    return table_list


def get_names(p):
    mapping = p.mapping
    names = []

    for k, v in mapping.items():
        if k == "aggregated_props":
            names.extend([{'name': i["name"]} for i in v])

        if k == "flatten_props":
            names.extend(chain(*[i["props"] for i in v]))

        if k == "props":
            names.extend(v)
    res = [v['name'] for v in names]
    print(res)
    return res


def get_doc_types(interpreter):
    return [dt.doc_type for dt in interpreter.translators]
