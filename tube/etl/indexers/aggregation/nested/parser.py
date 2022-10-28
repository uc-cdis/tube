from tube.utils.dd import get_edge_table, get_node_table_name, get_properties_types
from tube.utils.general import replace_dot_with_dash, get_node_id_name
from tube.etl.indexers.aggregation.nodes.nested_node import NestedNode
from tube.etl.indexers.base.parser import Parser as BaseParser


class Parser(BaseParser):
    """
    The main entry point into the index export process for the mutation indices
    """

    def __init__(self, mapping, model, dictionary):
        super(Parser, self).__init__(dictionary, mapping, model)
        self.leaves = []
        self.collectors = []
        self.root_node = None
        self.root_node = NestedNode(
            self.root.name,
            get_node_table_name(self.model, self.root.name),
            self.root.name,
            self.root.name,
            props=[],
        )
        self.get_nested_props(mapping)
        self.update_level()
        self.array_types = []
        self.collected_types = {}

    def get_nested_props(self, mapping):
        nested_indices = mapping.get("nested_props", [])
        for n_idx in nested_indices:
            self.root_node.children.add(
                self.parse_nested_props(n_idx, self.root_node, self.root.name)
            )

    def parse_nested_props(self, mapping, nested_parent_node, parent_label):
        path = mapping.get("path")
        path_components = path.split(".")
        parent_edge_up_tbls = []

        current_node_label = None
        current_parent_label = parent_label
        for p in path_components:
            current_node_label, edge_up_tbl = get_edge_table(
                self.model, current_parent_label, p
            )
            parent_edge_up_tbls.append((current_parent_label, edge_up_tbl))
            current_parent_label = current_node_label
        parent_edge_up_tbls.reverse()

        tbl_name = get_node_table_name(self.model, current_node_label)

        props = self.create_props_from_json(
            self.doc_type, mapping.get("props"), node_label=current_node_label
        )

        current_nested_node = NestedNode(
            current_node_label,
            tbl_name,
            path,
            mapping.get("name", replace_dot_with_dash(path)),
            props=props,
            parent_node=nested_parent_node,
            parent_edge_up_tbl=parent_edge_up_tbls,
            json_filter=mapping.get("filter"),
        )
        nested_idxes = mapping.get("nested_props", [])
        for n_idx in nested_idxes:
            current_nested_node.children.add(
                self.parse_nested_props(n_idx, current_nested_node, current_node_label)
            )

        if len(current_nested_node.children) == 0:
            self.leaves.append(current_nested_node)
        else:
            self.collectors.append(current_nested_node)

        return current_nested_node

    def update_level(self):
        """
        Update the level of nodes in the parsing tree
        :return:
        """
        level = 1
        assigned_levels = set([])
        just_assigned = set([])
        for child in self.root_node.children:
            if child in just_assigned:
                continue
            child.level = level
            if len(child.children) == 0:
                continue
            just_assigned.add(child)
        assigned_levels = assigned_levels.union(just_assigned)

        level += 1
        len_non_leaves = len(self.collectors)
        self.update_level_for_non_leaves(
            level, assigned_levels, just_assigned, len_non_leaves
        )

    def create_mapping_json(self, node, queue):
        es_type = {str: "keyword", float: "float", int: "long"}
        prop_types = get_properties_types(self.model, node.name)
        id_prop = get_node_id_name(node.name)
        properties = {}
        for p in node.props:
            p_type = self.select_widest_type(prop_types.get(p.src))
            properties[p.name] = {"type": es_type.get(p_type)}
            if p_type is str or p_type is bool:
                properties[p.name]["fields"] = {"analyzed": {"type": "text"}}

        properties[id_prop] = {
            "type": "keyword",
            "fields": {"analyzed": {"type": "text"}},
        }
        current_type = {node.display_name: {"properties": properties}}
        for child in node.children:
            if child.path in self.collected_types:
                child_types = self.collected_types.get(child.path)
                if "type" not in child_types[child.display_name]:
                    child_types[child.display_name]["type"] = "nested"
                properties.update(child_types)
        parent = node.parent_node
        if parent is not None:
            parent.children_ready_to_nest_types.append(node)
            if len(parent.children_ready_to_nest_types) == len(parent.children):
                queue.append(parent)
        return current_type

    def get_es_types(self):
        queue = []
        for l in self.leaves:
            queue.append(l)
        i: int = 0
        while i < len(queue):
            type = self.create_mapping_json(queue[i], queue)
            self.collected_types[queue[i].path] = type
            i += 1
        self.types = self.collected_types[queue[len(queue) - 1].path]
        self.update_array_types()
        return self.types

    def update_path_for_a_type(self, p_type, current_path):
        for k, v in p_type.get("properties").items():
            if v.get("type") == "nested":
                path_to_add = ".".join([p for p in (current_path, k) if p != ""])
                self.array_types.append(path_to_add)
                self.update_path_for_a_type(v, path_to_add)

    def update_array_types(self):
        for k, v in self.types.items():
            self.update_path_for_a_type(v, "")
