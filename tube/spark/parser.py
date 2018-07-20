import yaml
from tube.spark.node import Node, Reducer
from tube.utils import init_dictionary, get_edge_table, get_node_label, object_to_string


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
        self.root = list(self.mapping.keys())[0]
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
        # self.refresh_reversed_indices()
        print('list of nodes:\n{}\n'.format('\n'.join(str(i) for i in self.nodes)))
        print('leaves:\n{}\n'.format('\n'.join(str(i) for i in leaves)))
        # print('reversed indices: {}\n'.format(self.reversed_index))

    # def refresh_reversed_indices(self):
    #     self.reversed_index = {}
    #     for i in range(0, len(self.paths)):
    #         self.reversed_index[(self.paths[i].name, self.paths[i].edge_up_tbl)] = i

    def construct_reversed_parsing_tree(self, flat_paths):
        reversed_index = {}
        list_nodes = []
        for path in flat_paths:
            n_name = path.leaf
            p = path.path
            p_range = list(reversed(range(0, len(list(p)))))
            for i in p_range:
                parent, edge_up_tbl = get_edge_table(self.models, n_name, p[i])
                parent_name = get_node_label(self.models, parent)
                n = list_nodes[reversed_index[(n_name, edge_up_tbl)]] \
                    if (n_name, edge_up_tbl) in reversed_index \
                    else Node(n_name, edge_up_tbl)

                if n_name == path.leaf:
                    n.reducer = Reducer(None, path.fn, path.name)

                if (i > 0):
                    _, parent_edge_up_tbl = get_edge_table(self.models, parent_name, p[i-1])
                else:
                    parent_edge_up_tbl = None
                n_parent = list_nodes[reversed_index[(parent_name, parent_edge_up_tbl)]] \
                    if (parent_name, parent_edge_up_tbl) in reversed_index \
                    else Node(parent_name, parent_edge_up_tbl)
                n_parent.add_child(n)
                n.parent = n_parent

                if (n_name, edge_up_tbl) not in reversed_index:
                    list_nodes.append(n)
                    reversed_index[(n_name, edge_up_tbl)] = len(list_nodes) - 1
                if (parent_name, parent_edge_up_tbl) not in reversed_index:
                    list_nodes.append(n_parent)
                    reversed_index[(parent_name, parent_edge_up_tbl)] = len(list_nodes) - 1
                n_name = parent_name

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

    def get_mapping_func(self, leaf_func):
        if leaf_func == 'count':
            return 'sum'
        else:
            return leaf_func
