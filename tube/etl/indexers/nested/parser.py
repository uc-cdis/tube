from tube.utils.db import get_db_cursor
from tube.utils.dd import (
    get_edge_table,
    get_all_edges_table,
    get_node_label,
    get_parent_name,
    get_node_table_name,
    get_parent_label,
    get_all_children_of_node,
    object_to_string,
)
from tube.utils.dd import get_properties_types
from .nodes.nested_node import NestedNode
from ..base.parser import Parser as BaseParser


class Path(object):
    def __init__(self, roots, path_name, src):
        self.roots = roots
        self.src = src
        self.path = Path.create_path(path_name)

    @classmethod
    def create_path(cls, s_path):
        return tuple(s_path.split("."))

    def __key__(self):
        return (self.src,) + self.path

    def __hash__(self):
        return hash(self.__key__())

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return str(self.__key__())

    def __eq__(self, other):
        return self.__key__() == other.__key__()

    def __ne__(self, other):
        return self.__key__() != other.__key__()


class NodePath(object):
    def __init__(self, class_name, upper_path):
        self.class_name = class_name
        self.upper_path = upper_path

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return self.__str__()


class Parser(BaseParser):
    """
    The main entry point into the index export process for the mutation indices
    """

    def __init__(self, mapping, model, dictionary):
        if mapping is None:
            mapping = {
                "root": "read_group",
                "doc_type": "read_group",
                "name": "read_group",
            }
        super(Parser, self).__init__(mapping, model)
        self.dictionary = dictionary
        flat_paths = self.get_flat_paths()
        self.edges_with_data = self.get_edges_having_data()
        (
            self.leaves,
            self.collectors,
            self.roots,
        ) = self.create_tree_from_edges_with_data(
            flat_paths, self.get_props_for_nodes()
        )
        self.update_level()
        self.collectors.sort()
        self.collected_types = {}

    def get_flat_paths(self):
        def selected_category_comparer(model, x):
            return len(get_all_children_of_node(model, x)) == 0

        flat_paths = self.create_collecting_paths_from_root(
            self.root,
            lambda x: selected_category_comparer(
                self.model, self.model.Node.get_subclass(x).__name__
            ),
        )
        return flat_paths

    def get_props_for_nodes(self):
        roots = {}
        nested_node = NestedNode(self.root, get_node_table_name(self.model, self.root),)
        roots[self.root] = nested_node
        return roots

    def add_root_node(self, child, roots, segment):
        root_name = get_node_label(
            self.model, get_parent_name(self.model, child.name, segment)
        )
        _, edge_up_tbl = get_edge_table(self.model, child.name, segment)
        root_tbl_name = get_node_table_name(
            self.model, get_parent_label(self.model, child.name, segment)
        )
        top_node = (
            roots[root_name]
            if root_name in roots
            else NestedNode(
                root_name,
                root_tbl_name,
                self.create_props_from_json(
                    self.doc_type,
                    [{"name": "program_name", "src": "name"}],
                    node_label=root_name,
                    is_additional=True,
                ),
            )
        )
        child.add_parent(top_node, edge_up_tbl)
        top_node.add_child(child)
        roots[root_name] = top_node

    def add_collecting_node(self, child, collectors, fst):
        parent_name = get_node_label(
            self.model, get_parent_name(self.model, child.name, fst)
        )
        _, edge_up_tbl = get_edge_table(self.model, child.name, fst)
        tbl_name = get_node_table_name(
            self.model, get_parent_label(self.model, child.name, fst)
        )
        collecting_node = (
            collectors[parent_name]
            if parent_name in collectors
            else NestedNode(parent_name, tbl_name)
        )
        collecting_node.add_child(child)
        child.add_parent(collecting_node, edge_up_tbl)
        collectors[parent_name] = collecting_node
        return collecting_node

    def update_level(self):
        """
        Update the level of nodes in the parsing tree
        :return:
        """
        level = 1
        assigned_levels = set([])
        just_assigned = set([])
        for root in self.roots:
            for child in root.children:
                if child in just_assigned:
                    continue
                child.level = level
                if len(child.children) == 0:
                    continue
                just_assigned.add(child)
        assigned_levels = assigned_levels.union(just_assigned)

        level += 1
        leaves = [c for c in self.collectors if len(c.children) == 0]
        len_non_leaves = len(self.collectors) - len(leaves)
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

    def get_all_edge_tables_in_db(self):
        with get_db_cursor("db") as cur:
            db_name = cur.connection.info.dbname
            query_all_tb_names = (
                "select table_name from information_schema.tables "
                "where table_schema='public' and table_catalog='{dbname}' and table_name like 'edge_%';".format(
                    dbname=db_name
                )
            )
            cur.execute(query_all_tb_names)
            edge_tbs_in_db = cur.fetchall()
        return edge_tbs_in_db

    def get_edges_having_data(self):
        list_tb_names_from_dict = get_all_edges_table(self.model)
        tb_names = list(
            set(list_tb_names_from_dict)
            & set([i.get("table_name") for i in self.get_all_edge_tables_in_db()])
        )

        query = ", ".join(
            "(select count(*) from {tb_name}) as {tb_name}".format(tb_name=tb_name)
            for tb_name in tb_names
        )
        query_statement = "select {query};".format(query=query)
        with get_db_cursor("db") as cur:
            cur.execute(query_statement)
            count_values = cur.fetchone()

        tb_names_with_data = []
        for i in range(len(tb_names)):
            if count_values[i] > 0:
                tb_names_with_data.append(tb_names[i])
        return tb_names_with_data

    def initialize_queue(self, label):
        name = self.model.Node.get_subclass(label).__name__
        processing_queue = []
        children = get_all_children_of_node(self.model, name)
        for child in children:
            processing_queue.append(
                NodePath(child.__src_class__, child.__src_dst_assoc__)
            )
        return processing_queue

    def create_collecting_paths_from_root(self, label, selector):
        flat_paths = set()
        processing_queue = self.initialize_queue(label)
        i: int = 0
        while i < len(processing_queue):
            current_node = processing_queue[i]
            current_label = get_node_label(self.model, current_node.class_name)
            if selector(current_label):
                path = Path([label], current_node.upper_path, current_label)
                flat_paths.add(path)
            children = get_all_children_of_node(self.model, current_node.class_name)
            for child in children:
                processing_queue.append(
                    NodePath(
                        child.__src_class__,
                        ".".join([child.__src_dst_assoc__, current_node.upper_path]),
                    )
                )
            i += 1
        return flat_paths

    def create_tree_from_edges_with_data(self, flat_paths, roots):
        collectors = {}
        leaves = {}
        checking_set = set(self.edges_with_data)
        for p in flat_paths:
            segments = list(p.path)
            _, edge_up_tbl = get_edge_table(self.model, p.src, segments[0])
            if edge_up_tbl not in checking_set:
                continue
            if p.src not in collectors:
                tbl_name = get_node_table_name(self.model, p.src)
                collectors[p.src] = NestedNode(p.src, tbl_name)
            child = collectors[p.src]
            if p.src not in leaves:
                leaves[p.src] = child
            if len(segments) > 1:
                for fst in segments[0 : len(segments) - 1]:
                    _, edge_up_tbl = get_edge_table(self.model, p.src, segments[0])
                    if edge_up_tbl not in checking_set:
                        break
                    child = self.add_collecting_node(child, collectors, fst)
            self.add_root_node(child, roots, segments[-1])
        return set(leaves.values()), list(collectors.values()), list(roots.values())

    def create_mapping_json(self, node, queue):
        es_type = {str: "keyword", float: "float", int: "long"}
        prop_types = get_properties_types(self.model, node.name)
        properties = {
            prop_name: {"type": es_type.get(self.select_widest_type(prop_type))}
            for prop_name, prop_type in prop_types.items()
        }
        current_type = {node.name: {"properties": properties}}
        for child in node.children:
            if child.name in self.collected_types:
                child_types = self.collected_types.get(child.name)
                if "type" not in child_types[child.name]:
                    child_types[child.name]["type"] = "nested"
                properties.update(child_types)
        for name, parent in node.parent_nodes.items():
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
            self.collected_types[queue[i].name] = type
            i += 1
        self.types = self.collected_types[queue[len(queue) - 1].name]
