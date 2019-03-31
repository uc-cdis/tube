from tube.utils.dd import object_to_string
from tube.etl.indexers.base.node import BaseNode
from tube.etl.indexers.base.prop import PropFactory


class RootNode(BaseNode):
    def __init__(self, name, tbl_name, props, edge_to_parent=None):
        super(RootNode, self).__init__()
        self.name = name
        self.edge_to_parent = edge_to_parent
        self.tbl_name = tbl_name
        self.props = props
        self.root_child = None

    def __repr__(self):
        return self.name


class CollectingNode(BaseNode):
    def __init__(self, name, level=None):
        super(CollectingNode, self).__init__()
        self.name = name
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
        return 'Collecting: {}'.format(self.name)

    def __eq__(self, other):
        return self.level == other.level and \
               self.non_leaf_children_count == other.non_leaf_children_count

    def __lt__(self, other):
        return self.level < other.level or (
            self.level == other.level and self.non_leaf_children_count < other.non_leaf_children_count
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
        return 'Leaf: {}'.format(self.name)
