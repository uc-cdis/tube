from itertools import chain

from tests.value.value import Value
from tube.utils.dd import get_edge_table, get_node_table_name


class AggregatorValue(Value):
    def __init__(self, sql, parser, submitter_id, doc_type, names):
        super(AggregatorValue, self).__init__(parser, submitter_id, doc_type, names)
        self.sql = sql
        self.parser = parser
        self.submitter_id = submitter_id
        self.doc_type = doc_type
        self.names = names
        self.val = self.values()

    def __getattr__(self, item):
        return self.val[item] if item in self.val else None

    def value(self, name):
        path, value_mapping = self.get_path_by_name(self.parser, name)

        fn = path["fn"]
        src = path.get("src", path.get("name", None))

        tables = self.get_table_list_from_path(self.parser, self.doc_type, path["path"])
        val = self.sql[tables, fn, name, src, self.submitter_id]

        if value_mapping:
            if value_mapping.value_mappings:
                val = self.convert_mappings(val, value_mapping.value_mappings)

        return val

    def values(self):
        result = {name: self.value(name) for name in self.names}
        return result

    def get_table_list_from_path(self, p, root, path):
        r = []
        splitted_path = path.split(".") if path else []

        node = get_node_table_name(p.model, root)
        r.append(node)

        for i in splitted_path:
            root, node = get_edge_table(p.model, root, i)
            r.append(node)

            node = get_node_table_name(p.model, root)
            r.append(node)
        return r

    def get_path_by_name(self, p, name):
        mapping = p.mapping

        for k, v in list(mapping.items()):
            if k == "aggregated_props":
                filtered = [i for i in v if name == i["name"]]
                if filtered:
                    return filtered[0], None

            if k == "flatten_props":
                filtered = [i for i in v if name in [j["name"] for j in i["props"]]]
                value_mappings = [
                    x
                    for x in chain(*(x.props for x in p.flatten_props))
                    if name == x.name
                ]
                if filtered:
                    filtered[0]["fn"] = "_get"
                    return filtered[0], value_mappings[0]

            if k == "props":
                filtered = [i for i in v if name == i["name"]]
                value_mappings = [x for x in p.props if name == x.name]
                if filtered:
                    return {"path": "", "fn": "_get"}, value_mappings[0]
        return None

    def convert_mappings(self, value, value_mappings):
        """

        :param value:
        :param value_mappings:
        :return:
        """
        for v in value_mappings:
            if value == v.original:
                return v.final
        return value
