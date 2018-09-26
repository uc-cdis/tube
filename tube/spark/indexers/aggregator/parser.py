from tube.utils import get_attribute_from_path, get_edge_table, get_child_table, get_multiplicity,\
    get_node_table_name, get_properties_types, object_to_string, select_widest_types
from .nodes.aggregated_node import AggregatedNode, Reducer
from .nodes.direct_node import DirectNode
from ..base.parser import Parser as BaseParser


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


class Parser(BaseParser):
    """
    The main entry point into the index export process for the mutation indices
    """
    def __init__(self, mapping, model, dictionary):
        super(Parser, self).__init__(mapping, model)
        self.dictionary = dictionary
        self.props = self.get_root_props()
        self.flatten_props = self.get_direct_children() if '_flatten_props' in mapping else []
        self.aggregated_nodes = []
        if '_aggregated_props' in self.mapping:
            self.aggregated_nodes = self.get_aggregation_nodes()
        self.types = self.get_types()

    def get_root_props(self):
        return self.mapping['_props']

    def get_aggregation_nodes(self):
        flat_paths = self.create_paths()
        list_nodes, leaves = self.construct_aggregation_tree(flat_paths)

        aggregated_nodes = [l for l in list_nodes if l not in leaves]

        for p in aggregated_nodes:
            p.non_leaf_children_count = Parser.non_leaves_count(p.children, leaves)
        aggregated_nodes.sort()
        return aggregated_nodes

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
            n_name = self.mapping['root']
            current_parent_edge = None
            level = 0
            for i, p in enumerate(path.path):
                if (n_name, current_parent_edge) in reversed_index:
                    n_current = list_nodes[reversed_index[(n_name, current_parent_edge)]]
                else:
                    n_current = AggregatedNode(n_name, get_node_table_name(self.model, n_name),
                                               current_parent_edge, level)
                    list_nodes.append(n_current)
                    reversed_index[(n_name, current_parent_edge)] = len(list_nodes) - 1

                child_name, edge_tbl = get_edge_table(self.model, n_name, p)

                n_child = list_nodes[reversed_index[(child_name, edge_tbl)]] \
                    if (child_name, edge_tbl) in reversed_index \
                    else AggregatedNode(child_name, get_node_table_name(self.model, child_name), edge_tbl, level + 1)
                n_child.parent = n_current
                if i == len(path.path) - 1:
                    n_child.reducer = Reducer(None, path.fn, path.name)

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
        flat_paths = set()
        aggregated_nodes = self.mapping['_aggregated_props']
        for n in aggregated_nodes:
            path = Path(n['name'], n['path'], n['fn'])
            flat_paths.add(path)
        return flat_paths

    def get_direct_children(self):
        children = self.mapping['_flatten_props']
        nodes = []
        for child in children:
            parent, edge = get_edge_table(self.model, self.root, child['path'])
            child_name = get_child_table(self.model, self.root, child['path'])
            props = child['_props']

            multiplicity = get_multiplicity(self.dictionary, self.root, parent)

            if multiplicity == 'one_to_one':
                nodes.append(DirectNode(child_name, edge, props))
            else:
                raise Exception("something bad has just happened\n"
                                "the properties '{}' for '{}'\n"
                                "for parent '{}'\n"
                                "has multiplicity '{}'\n"
                                "you can't use on in '_flatten_props'\n".format(props, child['path'], parent,
                                                                                multiplicity))
        return nodes

    def get_types(self):
        mapping = self.mapping
        model = self.model
        root = self.root

        types = {}

        for k, v in mapping.items():
            if k == '_aggregated_props':
                types.update({i['name']: (float,) for i in v})

            if k == '_flatten_props':
                for i in v:
                    a = get_properties_types(model, get_attribute_from_path(model, root, i['path']))
                    for j in i['_props']:
                        if j in a:
                            types[j] = a[j]
                        else:
                            types[j] = (str,)

            if k == '_props':
                types.update({w: get_properties_types(model, root)[w] for w in v})

        types = select_widest_types(types)

        return types
