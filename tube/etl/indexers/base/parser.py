from tube.utils.general import get_node_id_name
from ..base.prop import PropFactory
from .node import BaseRootNode
from tube.utils.dd import get_properties_types
from tube.utils.dd import get_node_table_name
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    IntegerType,
    FloatType,
)


class Parser(object):
    """
    The main entry point into the index export process for the mutation indices
    """

    def __init__(self, mapping, model):
        self.mapping = mapping
        self.model = model
        self.name = mapping["name"]
        self.doc_type = mapping["doc_type"]
        if (
            mapping["root"] is not None
            and mapping["root"] != "None"
            and "props" in mapping
        ):
            self.root = BaseRootNode(
                name=mapping["root"],
                tbl_name=get_node_table_name(model, mapping["root"]),
                props=self.create_props_from_json(
                    self.doc_type, self.mapping["props"], node_label=mapping["root"]
                ),
            )
        else:
            self.root = None
        self.joining_nodes = []
        self.additional_props = []
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
        self.types = []

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
        self.types = {}
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
                self.types[p.name] = (
                    self.select_widest_type(p.type),
                    1 if array_type_condition else 0,
                )

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

    def get_prop_type(self, fn, src, node_label=None, index=None):
        if fn is not None:
            if fn in ["count", "sum", "min", "max"]:
                return (float,)
            elif fn in ["set", "list"]:
                return (str,)
            return (str,)
        elif index is not None:
            return None
        else:
            if node_label is None:
                raise Exception(
                    "An index property must have at least one of [path, fn, index] is set"
                )
            if src == "id":
                return (str,)
            a = get_properties_types(self.model, node_label)
            return a.get(src)

    def get_hadoop_type(self, prop):
        if prop.fn is not None and prop.fn in ["list", "set"]:
            return ArrayType(StringType())
        if prop.type == (float,):
            return FloatType()
        if prop.type == (str,):
            return StringType()
        if prop.type == (int,):
            return IntegerType()
        return StringType()

    @staticmethod
    def get_src_name(props):
        lst = [
            tuple([e.strip() for e in p.split(":")])
            if ":" in p
            else tuple([p.strip()] * 2)
            for p in props
        ]
        return lst

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
