import yaml
from tube.utils import init_dictionary


class MapReducePath(object):
    def __init__(self, current):
        self.parent = set()
        self.children = set()
        self.name = current
        self.non_leaf_children_count = 0

    def update_none_leaves_count(self, leaves):
        count = 0
        for n in self.children:
            if n not in leaves:
                count += 1
        self.non_leaf_children_count = count

    def __str__(self):
        if len(self.children) > 0:
            return '<name: {0}, parent: {1}, children: {2}, non_leaf_children_count: {3}>'\
                .format(self.name, self.parent, self.children, self.non_leaf_children_count)
        else:
            return '<name: {0}, parent: {1}>' \
                .format(self.name, self.parent)

    def __repr__(self):
        return self.__str__()

    def __lt__(self, other):
        return self.non_leaf_children_count < other.non_leaf_children_count

    def __eq__(self, other):
        return self.non_leaf_children_count == other.non_leaf_children_count


class Parser(object):
    """
    The main entry point into the index export process for the mutation indices
    """
    def __init__(self, file_path, url):
        stream = file(file_path)
        self.mapping = yaml.load(stream)
        self.dictionary, self.models = init_dictionary(url)
        self.reversed_index = {}
        self.paths = []
        self.leaves = {}
        self.load_parser_from_dict()

    def load_parser_from_dict(self):
        flat_paths = self.create_paths()
        self.reversed_index, list_paths, self.leaves = self.construct_reversed_parsing_tree(flat_paths)

        self.paths = [l for l in list_paths if l.name not in self.leaves]

        for p in self.paths:
            p.non_leaf_children_count = self.non_leaves_count(p.children, self.leaves)
        self.paths.sort()
        self.refresh_reversed_indices()
        # print('leaves: {}\n'.format(self.leaves))
        # print('paths: {}\n'.format(self.paths))
        # print('reversed indices: {}\n'.format(self.reversed_index))

    def refresh_reversed_indices(self):
        self.reversed_index = {}
        for i in range(0, len(self.paths)):
            self.reversed_index[self.paths[i].current] = i

    def construct_reversed_parsing_tree(self, flat_paths):
        reversed_index = {}
        list_paths = []
        for p in flat_paths:
            p_range = list(reversed(range(1, len(list(p)))))
            for i in p_range:
                n_child = list_paths[reversed_index[p[i]]] if p[i] in reversed_index else MapReducePath(p[i])
                n_parent = list_paths[reversed_index[p[i-1]]] if p[i-1] in reversed_index else MapReducePath(p[i-1])

                n_child.parent.add(p[i-1])
                n_parent.children.add(p[i])
                if p[i] not in reversed_index:
                    list_paths.append(n_child)
                    reversed_index[p[i]] = len(list_paths) - 1
                if p[i-1] not in reversed_index:
                    list_paths.append(n_parent)
                    reversed_index[p[i-1]] = len(list_paths) - 1

        return reversed_index, list_paths, self.get_leaves(list_paths)

    def get_leaves(self, list_paths):
        leaves = {}
        for l in list_paths:
            if len(l.children) == 0:
                leaves[l.current] = l
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
            flat_paths.add(tuple([root_key] + n['path'].split('.')))
        return flat_paths
