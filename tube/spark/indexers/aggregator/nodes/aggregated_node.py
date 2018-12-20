from tube.utils import object_to_string
from tube.spark.indexers.base.node import BaseNode


class AggregatedNode(BaseNode):
    def __init__(self, name, tbl_name, edge_up_tbl, level):
        super(AggregatedNode, self).__init__()
        self.name = name
        self.tbl_name = tbl_name
        self.edge_up_tbl = edge_up_tbl
        self.level = level
        self.parent = None
        self.non_leaf_children_count = 0
        self.reducer = None
        self.done = False

    def __key__(self):
        if self.edge_up_tbl is not None:
            return self.name, self.edge_up_tbl
        return self.name

    def __hash__(self):
        return hash(self.__key__())

    def __repr__(self):
        return '({}; {})'.format(str(self.__key__()), self.level)

    def __eq__(self, other):
        return self.level == other.level and \
               self.non_leaf_children_count == other.non_leaf_children_count

    def __lt__(self, other):
        return self.level > other.level or (
            self.level == other.level and self.non_leaf_children_count < other.non_leaf_children_count
        )


class Reducer(object):
    def __init__(self, prop, fn, output):
        self.prop = prop
        self.fn = fn
        self.output = output

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return self.__str__()