import yaml
from tube.utils import init_dictionary, get_edge_table, object_to_string, get_child_table, get_node_table_name
from .aggregated_node import AggregatedNode, Reducer
from .direct_node import DirectNode


class Path(object):
    def __init__(self, prop_name, path_name, fn):
        self.name = prop_name
        self.fn = fn
        self.path = Path.create_path(path_name)

    @classmethod
    def create_path(cls, s_path):
        return tuple(s_path.split('.'))

    def __key__(self):
        return self.path + (self.fn,)

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
        stream = open(file_path)
        self.mapping = yaml.load(stream)
        self.root = list(self.mapping.keys())[0]
        self.dictionary, self.models = init_dictionary(url)
        self.aggregated_nodes = []
        self.flatten_props = self.get_direct_children()
        self.props = []
        self.root_table = get_node_table_name(self.models, self.root)
        self.root_fields = self.mapping[self.root]['_props']
        self.load_parser_from_dict()
        print(self.aggregated_nodes)

    def load_parser_from_dict(self):
        flat_paths = self.create_paths()
        list_nodes, leaves = self.construct_reversed_parsing_tree(flat_paths)

        self.aggregated_nodes = [l for l in list_nodes if l not in leaves]

        for p in self.aggregated_nodes:
            p.non_leaf_children_count = self.non_leaves_count(p.children, leaves)
        print(self.aggregated_nodes)
        self.aggregated_nodes.sort()

    def construct_reversed_parsing_tree(self, flat_paths):
        """
        Construct the parsing tree that have the path to the parent node.
        The tree constructed from the set of flat paths.
        For every flat_path, this function parses though all the edges and creates a node of ()
        Args:
            - flat_paths: set of aggregation paths in the output document.

        """
        reversed_index = {}
        list_nodes = []
        for path in flat_paths:
            n_name = self.root
            current_parent_edge = None
            level = 0
            for i, p in enumerate(path.path):
                if (n_name, current_parent_edge) in reversed_index:
                    n_current = list_nodes[reversed_index[(n_name, current_parent_edge)]]
                else:
                    n_current = AggregatedNode(n_name, get_node_table_name(self.models, n_name),
                                               current_parent_edge, level)
                    list_nodes.append(n_current)
                    reversed_index[(n_name, current_parent_edge)] = len(list_nodes) - 1

                child_name, edge_tbl = get_edge_table(self.models, n_name, p)

                n_child = list_nodes[reversed_index[(child_name, edge_tbl)]] \
                    if (child_name, edge_tbl) in reversed_index \
                    else AggregatedNode(child_name, get_node_table_name(self.models, child_name), edge_tbl, level + 1)
                n_child.parent = n_current
                if i == len(path.path)-1:
                    n_child.reducer = Reducer(None, path.fn, path.name)

                n_current.add_child(n_child)
                if (child_name, edge_tbl) not in reversed_index:
                    list_nodes.append(n_child)
                    reversed_index[(child_name, edge_tbl)] = len(list_nodes) - 1

                n_name = child_name
                current_parent_edge = edge_tbl
                level+=1

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
        flat_paths = set()
        aggregated_nodes = self.mapping[self.root]['_aggregated_props']
        for n in aggregated_nodes:
            path = Path(n['name'], n['path'], n['fn'])
            flat_paths.add(path)
        return flat_paths

    def get_direct_children(self):
        children = self.mapping[self.root]['_flatten_props']
        nodes = []
        for child in children:
            _, edge = get_edge_table(self.models, self.root, child['path'])
            child_name = get_child_table(self.models, self.root, child['path'])
            props = child['_props']
            nodes.append(DirectNode(child_name, edge, props))
        return nodes
