from tube.utils import object_to_string


class Node(object):
    def __init__(self, name, edge_up_tbl):
        self.name = name
        self.parent = None
        self.edge_up_tbl = edge_up_tbl
        self.children = set([])
        self.reducer = None
        self.non_leaf_children_count = 0
        self.done = False
        self.no_children_to_map = 0

    def __key__(self):
        return (self.name, self.edge_up_tbl)

    def __hash__(self):
        return hash(self.__key__())

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return str(self.__key__())

    def __eq__(self, other):
        return self.non_leaf_children_count == other.non_leaf_children_count

    def __lt__(self, other):
        return self.non_leaf_children_count < other.non_leaf_children_count

    def add_child(self, node):
        self.children.add(node)
        self.no_children_to_map = len(self.children)


class Reducer(object):
    def __init__(self, prop, fn, output):
        self.prop = prop
        self.fn = fn
        self.output = output

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return self.__str__()
