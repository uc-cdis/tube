from tube.etl.indexers.base.node import BaseNode
from tube.utils.dd import object_to_string


class DirectNode(BaseNode):
    def __init__(
        self, name, edge, props, sorted_by=None, desc_order=False, props_from_child=True
    ):
        super().__init__()
        self.name = name
        self.tbl_name = name
        self.edge = edge
        self.props = props
        self.sorted_by = sorted_by
        self.desc_order = desc_order
        self.props_from_child = props_from_child

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return str(self.__str__())
