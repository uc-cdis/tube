from tube.etl.indexers.base.node import BaseNode


class NestedNode(BaseNode):
    def __init__(
        self,
        name,
        tbl_name,
        path,
        props=None,
        parent_node=None,
        parent_edge_up_tbl=None,
        level=None,
    ):
        super(NestedNode, self).__init__()
        props = [] if props is None else props
        self.name = name
        self.tbl_name = tbl_name
        self.props = props
        self.level = level
        self.path = path
        self.parent_edge_up_tbl = (
            [] if parent_edge_up_tbl is None else parent_edge_up_tbl
        )
        self.parent_node = parent_node
        self.non_leaf_children_count = 0
        self.children_ready_to_join = []
        self.children_ready_to_nest_types = []

    def __key__(self):
        if self.parent_edge_up_tbl is not None and len(self.parent_edge_up_tbl) > 0:
            return self.name, self.parent_edge_up_tbl[0]
        return self.name

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
