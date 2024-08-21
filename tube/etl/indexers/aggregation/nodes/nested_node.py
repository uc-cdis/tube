from tube.etl.indexers.base.node import BaseNode
from tube.etl.indexers.base.logic import create_filter_from_json


class NestedNode(BaseNode):
    def __init__(
        self,
        name,
        tbl_name,
        path,
        display_name,
        props=None,
        parent_node=None,
        parent_edge_up_tbl=None,
        level=None,
        json_filter=None,
    ):
        super(NestedNode, self).__init__()
        props = [] if props is None else props
        self.name = name
        self.tbl_name = tbl_name
        self.props = props
        self.level = level
        self.path = path
        self.display_name = display_name
        self.parent_edge_up_tbl = (
            [] if parent_edge_up_tbl is None else parent_edge_up_tbl
        )
        self.parent_nodes = [parent_node]
        self.non_leaf_children_count = 0
        self.children_ready_to_join = []
        self.children_ready_to_nest_types = []
        self.filter = create_filter_from_json(json_filter)

    def __key__(self):
        if self.parent_edge_up_tbl is not None and len(self.parent_edge_up_tbl) > 0:
            return self.name, self.parent_edge_up_tbl[0]
        return self.name

    def add_parent(self, parent_node):
        self.parent_nodes.append(parent_node)

    def __hash__(self):
        return hash(self.__key__())

    def __repr__(self):
        return "({}; {})".format(str(self.__key__()), self.level)

    def __eq__(self, other):
        return (
            self.level == other.level
            and self.non_leaf_children_count == other.non_leaf_children_count
        )

    def __lt__(self, other):
        return self.level > other.level or (
            self.level == other.level
            and self.non_leaf_children_count < other.non_leaf_children_count
        )
