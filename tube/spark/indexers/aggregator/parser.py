from tube.utils import get_attribute_from_path, get_edge_table, get_child_table, get_multiplicity,\
    get_node_table_name, get_properties_types, object_to_string, select_widest_types
from .nodes.aggregated_node import AggregatedNode, Reducer
from .nodes.direct_node import DirectNode
from ..base.parser import Parser as BaseParser
from ..base.prop import PropFactory


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
        self.flatten_props = self.get_direct_children() if 'flatten_props' in mapping else []
        self.aggregated_nodes = []
        if 'aggregated_props' in self.mapping:
            self.aggregated_nodes = self.get_aggregation_nodes()
        self.types = self.get_types()

    def get_root_props(self):
        return PropFactory.create_props_from_json(self.mapping['props'])

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
        aggregated_nodes = self.mapping['aggregated_props']
        for n in aggregated_nodes:
            path = Path(n['name'], n['path'], n['fn'])
            flat_paths.add(path)
        return flat_paths

    def parse_sorting(self, child):
        sorts = child['sorted_by'] if 'sorted_by' in child else None
        if sorts is None:
            return None, None
        sorts = sorts.split(",")
        if len(sorts) > 1:
            desc_order = sorts[1].strip() == 'desc'
        else:
            desc_order = False
        return sorts[0], desc_order

    def get_direct_children(self):
        """
        Parse etlMapping file and return a list of direct children from the root
        """
        children = self.mapping['flatten_props']
        nodes = []
        bypass = self.mapping.get(
            'settings', {}).get('bypass_multiplicity_check')
        for child in children:
            child_node, edge = get_edge_table(self.model, self.root, child['path'])
            child_name, is_child = get_child_table(self.model, self.root, child['path'])
            multiplicity = get_multiplicity(self.dictionary, self.root, child_node) if is_child else \
                get_multiplicity(self.dictionary, child_node, self.root)
            sorted_by, desc_order = self.parse_sorting(child)
            if not bypass and sorted_by is None and multiplicity != 'one_to_one' and multiplicity != 'one_to_many':
                raise Exception("something bad has just happened\n"
                                "the properties '{}' for '{}'\n"
                                "for parent '{}'\n"
                                "has multiplicity '{}' that cannot be used on in 'flatten_props'"
                                "\n".format(child['props'], child['path'], child_node, multiplicity))
            nodes.append(DirectNode(child_name, edge, child['props'], sorted_by, desc_order, is_child))
        return nodes

    def get_types(self):
        mapping = self.mapping
        model = self.model
        root = self.root

        types = {}

        for k, v in mapping.items():
            if k == 'aggregated_props':
                types.update({i['name']: (float,) for i in v})
            if k == 'flatten_props':
                for i in v:
                    a = get_properties_types(model, get_attribute_from_path(model, root, i['path']))
                    for j in i['props']:
                        p = self.get_prop(j)
                        if p.src in a:
                            types[p.name] = a[p.src]
                        else:
                            types[p.name] = (str,)

            if k == 'props':
                props = [self.get_prop(p_in_json) for p_in_json in v]
                types.update({p.name: get_properties_types(model, root)[p.src] for p in props})

        types = select_widest_types(types)

        return types
