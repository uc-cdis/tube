from tube.utils.dd import object_to_string
from tube.etl.indexers.base.node import BaseNode, BaseRootNode


class RootNode(BaseRootNode):
    def __init__(self, name, tbl_name, props, edge_to_parent=None):
        super(RootNode, self).__init__(name, tbl_name, props)
        self.edge_to_parent = edge_to_parent
        self.root_child = None

    def __repr__(self):
        return self.name


class CollectingNode(BaseNode):
    def __init__(self, name, tbl_name, props=None, level=None):
        super(CollectingNode, self).__init__()
        props = [] if props is None else props
        self.name = name
        self.tbl_name = tbl_name
        self.props = props
        self.level = level
        self.parents = {}
        self.non_leaf_children_count = 0
        self.no_parent_to_map = 0
        self.done = False
        self.is_empty = False

    def __key__(self):
        return self.name

    def __hash__(self):
        return hash(self.__key__())

    def __repr__(self):
        return "Collecting: {}".format(self.name)

    def __str__(self):
        return object_to_string(self)

    def __eq__(self, other):
        return (
            (self.level is None and other.level is None) or self.level == other.level
        ) and self.non_leaf_children_count == other.non_leaf_children_count

    def __lt__(self, other):
        if self.level is None and other.level is not None:
            return False
        elif other.level is None or self.level is None:
            return True
        return self.level < other.level or (
            self.level == other.level
            and self.non_leaf_children_count < other.non_leaf_children_count
        )

    def add_parent(self, parent_name, edge_up_tbl):
        self.parents[parent_name] = edge_up_tbl
        self.no_parent_to_map = len(self.parents)


class LeafNode(object):
    def __init__(self, name, tbl_name, fields=None):
        self.name = name
        self.tbl_name = tbl_name
        self.props = fields
        self.done = False
        self.no_parent_to_map = 0

    def __key__(self):
        return self.name

    def __hash__(self):
        return hash(self.__key__())

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return "Leaf: {}".format(self.name)
