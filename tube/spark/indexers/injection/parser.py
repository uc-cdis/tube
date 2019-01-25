from tube.utils import get_edge_table, get_node_table_name, get_node_label, get_parent_name, get_parent_label, \
    get_properties_types, object_to_string, select_widest_types, select_widest_type
from tube.spark.indexers.injection.nodes.collecting_node import CollectingNode, RootNode, LeafNode
from ..base.parser import Parser as BaseParser
from ..base.prop import Prop, PropFactory


class Path(object):
    def __init__(self, prop_name, path_name, fn):
        self.name = prop_name
        self.fn = fn
        self.path = Path.create_path(path_name)

    @classmethod
    def create_path(cls, s_path):
        return tuple(s_path.split('.'))

    def __key__(self):
        return (self.name,) + self.path

    def __hash__(self):
        return hash(self.__key__())

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return str(self.__key__())

    def __eq__(self, other):
        return self.__key__() == other.__key__()


class Parser(BaseParser):
    def __init__(self, mapping, model):
        super(Parser, self).__init__(mapping, model)
        self.props = PropFactory.create_props_from_json(self.mapping['props'])
        self.final_fields = [p for p in self.props]
        self.final_types = self.get_props_types()

        self.leaves = set([])
        self.collectors = []
        self.roots = set([])
        self.get_collecting_nodes()
        self.types = self.get_types()

    def get_props_types(self):
        types = {}
        for k, v in self.mapping.items():
            if k == 'target_nodes' and len(v) > 0:
                a = get_properties_types(self.model, v[0]['name'])
                for j in self.mapping['props']:
                    types[j['name']] = a[j['name']]

        types = select_widest_types(types)
        return types

    def get_collecting_nodes(self):
        flat_paths = self.create_collecting_paths()
        self.leaves, self.collectors, self.roots = self.construct_reversed_collection_tree(flat_paths)
        self.update_level()
        self.collectors.sort()

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
                if type(child) is LeafNode or child in just_assigned:
                    continue
                child.level = level
                just_assigned.add(child)
        assigned_levels = assigned_levels.union(just_assigned)
        print(just_assigned)

        level += 1
        while len(assigned_levels) != len(self.collectors):
            new_assigned = set([])
            for collector in just_assigned:
                for child in collector.children:
                    if type(child) is LeafNode or child in assigned_levels:
                        continue
                    child.level = level
                    new_assigned.add(child)
            just_assigned = new_assigned
            print(just_assigned)
            assigned_levels = assigned_levels.union(new_assigned)
            level += 1

    def update_final_fields(self, root_name):
        for f in self.mapping['injecting_props'][root_name]['props']:
            src = f['src'] if 'src' in f else f['name']
            p = Prop(f['name'], src, [])
            if p.src != 'id':
                f_type = select_widest_type(get_properties_types(self.model, root_name)[p.src])
            else:
                f_type = str
            self.final_fields.append(p)
            self.final_types[p.name] = f_type

    def add_root_node(self, child, roots, segment):
        root_name = get_node_label(self.model, get_parent_name(self.model, child.name, segment))
        root_tbl_name = get_node_table_name(self.model, get_parent_label(self.model, child.name, segment))
        top_node = roots[root_name] if root_name in roots \
            else RootNode(root_name, root_tbl_name,
                          self.mapping['injecting_props'][root_name]['props']
                          )
        child.add_parent(top_node)
        top_node.add_child(child)
        if root_name not in roots:
            self.update_final_fields(root_name)

        roots[root_name] = top_node

    def add_collecting_node(self, child, collectors, fst, snd):
        parent_name = get_node_label(self.model, get_parent_name(self.model, child.name, fst))
        _, edge_up_tbl = get_edge_table(self.model, parent_name, snd)
        collecting_node = collectors[parent_name] if parent_name in collectors \
            else CollectingNode(parent_name, edge_up_tbl)
        collecting_node.add_child(child)
        child.add_parent(collecting_node)
        collectors[parent_name] = collecting_node
        return collecting_node

    def add_leaf_node(self, name, leaves, segment):
        leaf_tbl_name = get_node_table_name(self.model, name)
        _, edge_up_tbl = get_edge_table(self.model, name, segment)
        leaf_node = LeafNode(name, leaf_tbl_name, edge_up_tbl)
        leaves.add(leaf_node)
        return leaf_node

    def construct_reversed_collection_tree(self, flat_paths):
        leaves = set([])
        collectors = {}
        roots = {}
        for p in flat_paths:
            segments = list(p.path)
            child = self.add_leaf_node(p.name, leaves, segments[0])
            if len(segments) > 1:
                looping_list = zip(segments[0:len(segments)-1], segments[1:len(segments)])
                for fst, snd in looping_list:
                    child = self.add_collecting_node(child, collectors, fst, snd)
            self.add_root_node(child, roots, segments[-1])
        return leaves, collectors.values(), roots.values()

    def create_collecting_paths(self):
        flat_paths = set()
        target_nodes = self.mapping['target_nodes']
        for n in target_nodes:
            path = Path(n['name'], n['path'], None)
            flat_paths.add(path)
        return flat_paths

    def get_types(self):
        return self.final_types
