import re
from tube.utils.dd import (
    get_edge_table,
    get_child_table,
    get_multiplicity,
    get_node_table_name,
    object_to_string,
)
from .nodes.aggregated_node import AggregatedNode, Reducer
from .nodes.direct_node import DirectNode
from .nodes.joining_node import JoiningNode
from .nodes.special_node import SpecialNode, SpecialChain
from .nodes.parent_node import ParentChain, ParentNode
from ..base.parser import Parser as BaseParser
from copy import deepcopy
from tube.utils.general import PROJECT_CODE, PROGRAM_NAME


class Path(object):
    def __init__(self, path_name, reducer_as_json):
        self.reducers = [reducer_as_json]
        self.path = Path.create_path(path_name)

    @classmethod
    def create_path(cls, s_path):
        return tuple(s_path.split("."))

    def __key__(self):
        return self.path

    def __hash__(self):
        return hash(self.__key__())

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return str(self.__key__())

    def __eq__(self, other):
        return self.__key__() == other.__key__()


class Parser(BaseParser):
    """
    The main entry point into the index export process for the mutation indices
    """

    def __init__(self, mapping, model, dictionary):
        super(Parser, self).__init__(mapping, model)
        self.reducer_by_prop = {}
        self.dictionary = dictionary
        self.props = self.get_host_props()
        self.flatten_props = (
            self.get_direct_children() if "flatten_props" in mapping else []
        )
        self.aggregated_nodes = []
        if "aggregated_props" in self.mapping:
            self.aggregated_nodes = self.get_aggregation_nodes()
        self.joining_nodes = (
            self.get_joining_nodes()
            if "joining_props" in self.mapping or "joining" in self.mapping
            else []
        )
        self.special_nodes = (
            self.get_special_node() if "special_props" in self.mapping else []
        )
        self.parent_nodes = self.get_parent_props()

    def json_to_parent_node(self, path):
        words = path.split(".")
        nodes = [tuple([_f for _f in re.split("[\[\]]", w) if _f]) for w in words]
        first = None
        prev = None
        prev_label = self.root.name
        for nd in nodes:
            n = nd[0]
            p = nd[1] if len(nd) > 1 else None
            parent_name, edge_tbl = get_edge_table(self.model, prev_label, n)
            parent_tbl = get_node_table_name(self.model, parent_name)
            if p is not None:
                json_props = [
                    {"name": p[0], "src": p[1]} for p in self.get_src_name(p.split(","))
                ]
                props = self.create_props_from_json(
                    self.doc_type, json_props, node_label=parent_name
                )
            else:
                props = []
            cur = ParentNode(parent_name, parent_tbl, edge_tbl, props)
            if prev is not None:
                prev.child = cur
            else:
                first = cur
            prev_label = parent_name
            prev = cur
        return first

    def get_parent_props(self):
        list_nodes = []
        json_parents = self.mapping.get("parent_props", [])
        for r in json_parents:
            if "path" in r:
                list_nodes.append(
                    ParentChain(self.json_to_parent_node(r.get("path")), r.get("fn"))
                )
        return list_nodes

    def add_program_name_to_parent(self):
        """
        In case program name is not in self.mapping["parent_props"] while the root node is project. We must add that field
        :return:
        """
        found_program = -1
        i = -1
        for path in self.mapping["parent_props"]:
            p = path["path"]
            i += 1
            if p.startswith("program"):
                found_program = i
                break
        if found_program == -1:
            self.mapping["parent_props"].append(
                {"path": "programs[{PROGRAM_N}:name]".format(PROGRAM_N=PROGRAM_NAME)}
            )
            found_program = len(self.mapping["parent_props"]) - 1
        program_path = self.mapping["parent_props"][found_program]
        program_path_val = program_path["path"]
        if program_path_val.find(PROGRAM_NAME) == -1:
            separator = "" if program_path_val.find("[]") > 0 else ","
            program_path["path"] = "{}{SEP}{PROGRAM_N}:name]".format(
                program_path_val[: len(program_path) - 1],
                SEP=separator,
                PROGRAM_N=PROGRAM_NAME,
            )

    def get_host_props(self):
        if self.root.name == "project":
            if "project_code" not in [p.get("name") for p in self.mapping["props"]]:
                self.mapping["props"].append({"name": PROJECT_CODE, "src": "code"})
            if "parent_props" not in self.mapping:
                self.mapping["parent_props"] = []
            self.add_program_name_to_parent()

        return self.create_props_from_json(
            self.doc_type, self.mapping["props"], node_label=self.root.name
        )

    def get_aggregation_nodes(self):
        """
        Get aggregation nodes of aggregation tree which will produce aggregated_props
        :return:
        """
        flat_paths = self.create_paths()
        for p in flat_paths:
            print(str(p))
        list_nodes, leaves = self.construct_aggregation_tree(flat_paths)

        aggregated_nodes = [l for l in list_nodes if l not in leaves]

        for p in aggregated_nodes:
            p.non_leaf_children_count = Parser.non_leaves_count(p.children, leaves)
        aggregated_nodes.sort()
        return aggregated_nodes

    def json_to_special_node(self, path):
        """
        Create node in the path of special aggregation
        :param path: path define the node and the prop to be aggregated
        :return:
        """
        words = path.split(".")
        nodes = [tuple([_f for _f in re.split("[\[\]]", w) if _f]) for w in words]
        first = None
        prev = None
        prev_label = self.root.name
        for (n, str_p) in nodes:
            child_name, edge_tbl = get_edge_table(self.model, prev_label, n)
            child_tbl = get_node_table_name(self.model, child_name)
            json_props = [{"name": p, "src": p} for p in str_p.split(",")]
            props = self.create_props_from_json(
                self.doc_type, json_props, node_label=child_name
            )
            cur = SpecialNode(child_name, child_tbl, edge_tbl, props)
            if prev is not None:
                prev.child = cur
            else:
                first = cur
            prev_label = child_name
            prev = cur
        return first

    def get_special_node(self):
        """
        Parse definition of special aggregation and create aggregated node for that
        :return:
        """
        lst_nodes = []
        for s in self.mapping.get("special_props"):
            if "path" in s:
                lst_nodes.append(
                    SpecialChain(
                        self.doc_type,
                        s.get("name"),
                        self.json_to_special_node(s.get("path")),
                        s.get("fn", "").split(","),
                    )
                )
        return lst_nodes

    def get_joining_nodes(self):
        """
        Parse definition of joining between two indices
        :return:
        """
        joining_nodes = []
        joining_props = self.mapping.get("joining_props", self.mapping.get("joining"))
        for idx in joining_props:
            json_props = [
                {"name": j.get("name"), "src": j.get("src"), "fn": j.get("fn")}
                for j in idx["props"]
            ]
            props = self.create_props_from_json(
                self.doc_type, json_props, index=idx.get("index")
            )
            joining_nodes.append(JoiningNode(props, idx))
        return joining_nodes

    """
    Construct the parsing tree that have the path to the parent node.
    The tree constructed from the set of flat paths.
    For every flat_path, this function parses though all the edges and creates a node of ()
    Args:
        - flat_paths: set of aggregation paths in the output document.

    """

    def construct_aggregation_tree(self, flat_paths):
        reversed_index = {}
        list_nodes = []
        for path in flat_paths:
            n_name = self.mapping["root"]
            current_parent_edge = None
            level = 0
            for i, p in enumerate(path.path):
                if (n_name, current_parent_edge) in reversed_index:
                    n_current = list_nodes[
                        reversed_index[(n_name, current_parent_edge)]
                    ]
                else:
                    n_current = AggregatedNode(
                        n_name,
                        get_node_table_name(self.model, n_name),
                        current_parent_edge,
                        level,
                    )
                    list_nodes.append(n_current)
                    reversed_index[(n_name, current_parent_edge)] = len(list_nodes) - 1

                child_name, edge_tbl = get_edge_table(self.model, n_name, p)

                n_child = (
                    list_nodes[reversed_index[(child_name, edge_tbl)]]
                    if (child_name, edge_tbl) in reversed_index
                    else AggregatedNode(
                        child_name,
                        get_node_table_name(self.model, child_name),
                        edge_tbl,
                        level + 1,
                    )
                )
                n_child.parent = n_current
                if i == len(path.path) - 1:
                    for reducer in path.reducers:
                        prop = self.create_prop_from_json(self.doc_type, reducer, None)
                        n_child.reducers.append(Reducer(prop, reducer["fn"]))
                        self.reducer_by_prop[prop.name] = reducer["fn"]

                n_current.add_child(n_child)
                if (child_name, edge_tbl) not in reversed_index:
                    list_nodes.append(n_child)
                    reversed_index[(child_name, edge_tbl)] = len(list_nodes) - 1

                n_name = child_name
                current_parent_edge = edge_tbl
                level += 1

        return list_nodes, Parser.get_leaves(list_nodes)

    @classmethod
    def get_leaves(cls, list_paths):
        leaves = set([])
        for l in list_paths:
            if len(l.children) == 0:
                leaves.add(l)
        return leaves

    @classmethod
    def non_leaves_count(self, set_to_count, leaves):
        count = 0
        for n in set_to_count:
            if n not in leaves:
                count += 1
        return count

    def create_paths(self):
        """
        create all possible paths from mapping file.
        :return:
        """
        flat_paths = {}
        aggregated_nodes = self.mapping["aggregated_props"]
        for n in aggregated_nodes:
            copied_n = deepcopy(n)
            if "src" not in copied_n:
                copied_n["src"] = None
            path = copied_n.pop("path", None)
            if path in flat_paths:
                flat_paths[path].reducers.append(copied_n)
            else:
                flat_paths[path] = Path(path, copied_n)
        print(list(flat_paths.values()))
        for p in flat_paths:
            print(str(p))
        return set(flat_paths.values())

    def parse_sorting(self, child):
        sorts = child["sorted_by"] if "sorted_by" in child else None
        if sorts is None:
            return None, None
        sorts = sorts.split(",")
        if len(sorts) > 1:
            desc_order = sorts[1].strip() == "desc"
        else:
            desc_order = False
        return sorts[0], desc_order

    def get_direct_children(self):
        """
        Parse etlMapping file and return a list of direct children from the root
        """
        children = self.mapping["flatten_props"]
        nodes = []
        bypass = self.mapping.get("settings", {}).get("bypass_multiplicity_check")
        for child in children:
            child_label, edge = get_edge_table(
                self.model, self.root.name, child["path"]
            )
            child_name, is_child = get_child_table(
                self.model, self.root.name, child["path"]
            )
            multiplicity = (
                get_multiplicity(self.dictionary, self.root.name, child_label)
                if is_child
                else get_multiplicity(self.dictionary, child_label, self.root.name)
            )
            sorted_by, desc_order = self.parse_sorting(child)
            if (
                not bypass
                and sorted_by is None
                and multiplicity != "one_to_one"
                and multiplicity != "one_to_many"
            ):
                raise Exception(
                    "something bad has just happened\n"
                    "the properties '{}' for '{}'\n"
                    "for parent '{}'\n"
                    "has multiplicity '{}' that cannot be used on in 'flatten_props'"
                    "\n".format(
                        child["props"], child["path"], child_label, multiplicity
                    )
                )
            props = self.create_props_from_json(
                self.doc_type, child["props"], node_label=child_label
            )
            nodes.append(
                DirectNode(
                    child_label,
                    child_name,
                    edge,
                    props,
                    sorted_by,
                    desc_order,
                    is_child,
                )
            )
        return nodes
