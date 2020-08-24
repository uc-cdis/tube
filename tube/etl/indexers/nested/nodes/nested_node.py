from tube.utils.dd import object_to_string
from tube.etl.indexers.base.node import BaseNode


class NestedNode(BaseNode):
    def __init__(self, name, tbl_name, props=None, level=None):
        super(NestedNode, self).__init__()
        props = [] if props is None else props
        self.name = name
        self.tbl_name = tbl_name
        self.props = props
        self.level = level
        self.parent_edge_up_tbl = {}
        self.parent_nodes = {}
        self.non_leaf_children_count = 0
        self.children_ready_to_join = []
        self.children_ready_to_nest_types = []

    def __key__(self):
        return self.name

    def __hash__(self):
        return hash(self.__key__())

    def __repr__(self):
        return "Nested: {}".format(self.name)

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

    def add_parent(self, parent_node, edge_up_tbl):
        self.parent_edge_up_tbl[parent_node.name] = edge_up_tbl
        self.parent_nodes[parent_node.name] = parent_node
