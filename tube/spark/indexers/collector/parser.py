import yaml
from tube.utils import init_dictionary, get_edge_table, \
    object_to_string, get_node_table_name, get_node_label, get_parent_name, get_parent_label
from tube.spark.indexers.collector.nodes.collecting_node import CollectingNode, RootNode, LeafNode


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


class Parser(object):
    def __init__(self, file_path, url):
        stream = open(file_path)
        self.mapping = yaml.load(stream)
        self.root = list(self.mapping.keys())[0]
        self.dictionary, self.models = init_dictionary(url)
        self.common_targeting_fields = self.mapping[self.root]['_collecting_props']['_common_targeting_props']
        self.leaves = set([])
        self.collectors = []
        self.roots = {}
        self.get_collecting_nodes()

    def get_collecting_nodes(self):
        flat_paths = self.create_collecting_paths()
        self.leaves, self.collectors, self.roots = self.construct_reversed_collection_tree(flat_paths)
        self.update_level()
        self.collectors.sort()

        print(self.leaves)
        print('Collecting')
        for v in self.collectors:
            print(str(v))
        print('Root')
        for (k, v) in self.roots.items():
            print(str(v))

    def update_level(self):
        level = 1
        assigned_levels = set([])
        just_assigned = set([])
        for root in self.roots.values():
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

    def add_root_node(self, child, roots, segment):
        root_name = get_node_label(self.models, get_parent_name(self.models, child.name, segment))
        root_tbl_name = get_node_table_name(self.models, get_parent_label(self.models, child.name, segment))
        top_node = roots[root_name] if root_name in roots \
            else RootNode(root_name, root_tbl_name,
                          self.mapping[self.root]['_collecting_props']['_common_aggregating_props'][root_name]['_props']
                          )
        child.add_parent(top_node)
        top_node.add_child(child)
        roots[root_name] = top_node

    def add_collecting_node(self, child, collectors, fst, snd):
        parent_name = get_node_label(self.models, get_parent_name(self.models, child.name, fst))
        _, edge_up_tbl = get_edge_table(self.models, parent_name, snd)
        collecting_node = collectors[parent_name] if parent_name in collectors \
            else CollectingNode(parent_name, edge_up_tbl)
        collecting_node.add_child(child)
        child.add_parent(collecting_node)
        collectors[parent_name] = collecting_node
        return collecting_node

    def add_leaf_node(self, name, leaves, segment):
        leaf_tbl_name = get_node_table_name(self.models, name)
        edge_up_tbl = get_edge_table(self.models, name, segment)
        leaf_node = LeafNode(name, leaf_tbl_name, edge_up_tbl)
        leaves.add(leaf_node)
        return leaf_node

    def construct_reversed_collection_tree(self, flat_paths):
        leaves = set([])
        collectors = {}
        roots = {}
        for p in flat_paths:
            segments = list(p.path)
            print('path: {}'.format(p))
            child = self.add_leaf_node(p.name, leaves, segments[0])
            if len(segments) > 1:
                looping_list = zip(segments[0:len(segments)-1], segments[1:len(segments)])
                for fst, snd in looping_list:
                    child = self.add_collecting_node(child, collectors, fst, snd)
            self.add_root_node(child, roots, segments[-1])
        return leaves, collectors.values(), roots

    def create_collecting_paths(self):
        flat_paths = set()
        target_nodes = self.mapping[self.root]['_collecting_props']['_target_nodes']
        for n in target_nodes:
            path = Path(n['name'], n['path'], None)
            flat_paths.add(path)
        return flat_paths
