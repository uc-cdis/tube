from tube.utils.general import get_node_id_name
from ..base.prop import PropFactory
from ..base.filter import Filter
from tube.utils.dd import get_properties_types


class Parser(object):
    """
    The main entry point into the index export process for the mutation indices
    """

    def __init__(self, dictionary, mapping, model):
        self.dictionary = dictionary
        self.mapping = mapping
        self.model = model
        self.name = mapping["name"]
        self.root = mapping["root"]
        self.doc_type = mapping["doc_type"]
        self.joining_nodes = []
        self.additional_props = []
        self.array_types = []
        self.prop_types_from_dictionary = {}
        PropFactory.adding_prop(
            self.doc_type,
            get_node_id_name(self.doc_type),
            "",
            [],
            src_node=None,
            src_index=None,
            fn=None,
            prop_type=(str,),
        )
        self.prop_types = {}
        self.types = {}

    def get_possible_properties_types(self, models, node_label):
        if node_label not in self.prop_types_from_dictionary:
            return get_properties_types(models=models, node_name=node_label)
        return self.prop_types_from_dictionary.get(node_label)

    @staticmethod
    def generate_es_mapping_types(doc_name, field_types):
        """
        :param doc_name: name of the Elasticsearch document to create mapping for
        :param field_types: dictionary of field and their types
        :return: JSON with proper mapping to be used in Elasticsearch
        """
        es_type = {str: "keyword", float: "float", int: "long"}

        properties = {
            k: {"type": es_type[v[0]]}
            if v[0] is not str
            else {"type": es_type[v[0]], "fields": {"analyzed": {"type": "text"}}}
            for k, v in list(field_types.items())
        }

        # explicitly mapping 'node_id'
        properties["node_id"] = {"type": "keyword"}

        return {doc_name: {"properties": properties}}

    def update_prop_types(self):
        """
        The reason for the update_prop_types function is:
        - There are some properties/fields created by joining/getting value from another index/document
        Then, their type depends on the type of fields in original indices.
        During the first iteration the parser doesn't have all types in all the indices.
        Then, an additional update should be perform at the end.
        """
        props = list(PropFactory.get_prop_by_doc_name(self.doc_type).values())
        for p in props:
            if p.type is None:
                prop = PropFactory.get_prop_by_name(p.src_index, p.src)
                if prop is None:
                    continue
                p.update_type(prop.type)

    def get_es_types(self):
        types = {}
        types_to_convert_to_es_types = list(
            PropFactory.get_prop_by_doc_name(self.doc_type).values()
        )
        if PropFactory.get_additional_prop_by_doc_name(self.doc_type) is not None:
            types_to_convert_to_es_types += list(
                PropFactory.get_additional_prop_by_doc_name(self.doc_type).values()
            )
        for p in types_to_convert_to_es_types:
            if p.type is not None:
                is_array_type = p.type[0] == list
                has_array_agg_fn = p.fn is not None and p.fn in ["set", "list"]
                array_type_condition = is_array_type or has_array_agg_fn
                if array_type_condition:
                    types[p.name] = (
                        self.select_widest_type(p.type),
                        1,
                    )
                    if p.name not in self.array_types:
                        self.array_types.append(p.name)
                else:
                    types[p.name] = (
                        self.select_widest_type(p.type),
                        0,
                    )
        self.prop_types = types
        self.types = self.generate_es_mapping_types(self.doc_type, types)
        return self.types

    def select_widest_type(self, types):
        if str in types:
            return str
        elif float in types:
            return float
        elif int in types:
            return int
        else:
            return str

    def get_key_prop(self):
        return PropFactory.get_prop_by_name(
            self.doc_type, get_node_id_name(self.doc_type)
        )

    def get_prop_by_name(self, name):
        prop = PropFactory.get_prop_by_name(self.doc_type, name)
        if not prop:
            print("DEBUG: prop '{}' not in '{}'".format(name, self.doc_type))
        return prop

    def get_prop_type_of_field_in_dictionary(self, node_label, prop):
        dict_types = {"number": float, "string": str, "integer": int}
        node_prop = self.dictionary.schema.get(node_label).get("properties").get(prop)
        return list, dict_types.get(node_prop.get("items").get("type"))

    def get_prop_type(self, fn, src, node_label=None, index=None):
        if fn is not None and index is None:
            if fn in ["count", "sum", "min", "max"]:
                return float,
            elif fn in ["set", "list"]:
                if node_label is not None:
                    a = self.get_possible_properties_types(self.model, node_label)
                    if src == "id":
                        return str,
                    if a.get(src) == (list,):
                        return self.get_prop_type_of_field_in_dictionary(node_label, src)
                    return a.get(src)
                return str,
            return str,
        elif index is not None:
            index_prop = PropFactory.get_prop_by_name(index, src)
            if index_prop:
                return index_prop.type
            return None
        else:
            if node_label is None:
                raise Exception(
                    "An index property must have at least one of [path, fn, index] is set"
                )
            if src == "id":
                return str,
            a = self.get_possible_properties_types(self.model, node_label)
            if a.get(src) == (list, ):
                return self.get_prop_type_of_field_in_dictionary(node_label, src)
            return a.get(src)

    @staticmethod
    def get_src_name(props):
        lst = [
            tuple([e.strip() for e in p.split(":")])
            if ":" in p
            else tuple([p.strip()] * 2)
            for p in props
        ]
        return lst

    def get_filter(self, json_prop):
        return Filter(json_prop.get("filters"))

    def create_prop_from_json(
        self, doc_name, p, node_label=None, index=None, is_additional=False
    ):
        value_mappings = p.get("value_mappings", [])
        src = p["src"] if "src" in p else p["name"]
        fn = p.get("fn")


        prop_type = self.get_prop_type(fn, src, node_label=node_label, index=index)
        prop = PropFactory.adding_prop(
            doc_name,
            p["name"],
            src,
            value_mappings,
            src_node=node_label,
            src_index=index,
            fn=fn,
            prop_type=prop_type,
            is_additional=is_additional,
        )
        return prop

    def create_props_from_json(
        self, doc_name, props_in_json, node_label=None, index=None, is_additional=False
    ):
        res = []
        for p in props_in_json:
            res.append(
                self.create_prop_from_json(
                    doc_name,
                    p,
                    node_label=node_label,
                    index=index,
                    is_additional=is_additional,
                )
            )
        return res

    @staticmethod
    def update_level_for_non_leaves(
        level, assigned_levels, just_assigned, len_non_leaves
    ):
        while len(assigned_levels) <= len_non_leaves and len(just_assigned) > 0:
            new_assigned = set([])
            for collector in just_assigned:
                for child in collector.children:
                    if child in assigned_levels:
                        continue
                    child.level = level
                    if len(child.children) == 0:
                        continue
                    new_assigned.add(child)
            just_assigned = new_assigned
            assigned_levels = assigned_levels.union(new_assigned)
            level += 1
