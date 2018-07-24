import yaml
from tube.spark.node import Node, Reducer
from tube.utils import init_dictionary, get_edge_table, object_to_string


class Path(object):
    def __init__(self, prop_name, leaf, path_name, fn):
        self.name = prop_name
        self.fn = fn
        self.leaf = leaf
        self.path = Path.create_path(path_name)

    @classmethod
    def create_path(cls, s_path):
        return tuple(s_path.split('.'))

    def __key__(self):
        return self.path + (self.leaf,) + (self.fn,)

    def __hash__(self):
        return hash(self.__key__())

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return str(self.__key__())

    def __eq__(self, other):
        return self.__key__() == other.__key__()


class Parser(object):
    """
    The main entry point into the index export process for the mutation indices
    """
    def __init__(self, file_path, url):
        stream = file(file_path)
        self.mapping = yaml.load(stream)
        self.root = list(self.mapping.keys())[0].strip('\n')
        self.dictionary, self.models = init_dictionary(url)
        self.nodes = []
        self.load_parser_from_dict()

    def load_parser_from_dict(self):
        flat_paths = self.create_paths()
        print('flat_paths:\n{}\n'.format('\n'.join(str(i) for i in flat_paths)))
        list_nodes, leaves = self.construct_reversed_parsing_tree(flat_paths)

        self.nodes = [l for l in list_nodes if l not in leaves]

        for p in self.nodes:
            p.non_leaf_children_count = self.non_leaves_count(p.children, leaves)
        self.nodes.sort()

    def construct_reversed_parsing_tree(self, flat_paths):
        reversed_index = {}
        list_nodes = []
        for path in flat_paths:
            n_name = self.root
            current_parent_edge = None
            for p in path.path:
                if (n_name, current_parent_edge) in reversed_index:
                    n_current = list_nodes[reversed_index[(n_name, current_parent_edge)]]
                else:
                    n_current = Node(n_name, current_parent_edge)
                    list_nodes.append(n_current)
                    reversed_index[(n_name, current_parent_edge)] = len(list_nodes) - 1

                child_name, edge_tbl = get_edge_table(self.models, n_name, p)

                n_child = list_nodes[reversed_index[(child_name, edge_tbl)]] \
                    if (child_name, edge_tbl) in reversed_index \
                    else Node(child_name, edge_tbl)
                n_child.parent = n_current
                if child_name == path.leaf:
                    n_child.reducer = Reducer(None, path.fn, path.name)

                n_current.add_child(n_child)
                if (child_name, edge_tbl) not in reversed_index:
                    list_nodes.append(n_child)
                    reversed_index[(child_name, edge_tbl)] = len(list_nodes) - 1

                n_name = child_name
                current_parent_edge = edge_tbl

        return list_nodes, self.get_leaves(list_nodes)

    def get_leaves(self, list_paths):
        leaves = set([])
        for l in list_paths:
            if len(l.children) == 0:
                leaves.add(l)
        return leaves

    def non_leaves_count(self, set_to_count, leaves):
        count = 0
        for n in set_to_count:
            if n not in leaves:
                count += 1
        return count

    def create_paths(self):
        root_key = list(self.mapping.keys())[0]
        flat_paths = set()
        aggregated_nodes = self.mapping[root_key]['_aggregated_props']
        for n in aggregated_nodes:
            path = Path(n['name'], n['leaf'], n['path'], n['fn'])
            flat_paths.add(path)
        return flat_paths
