from tube.utils import object_to_string
from tube.spark.indexers.base.prop import PropFactory


class DirectNode(object):
    def __init__(self, name, edge, props_in_json, props_from_child=True):
        self.tbl_name = name
        self.edge = edge
        self.props = PropFactory.create_props_from_json(props_in_json)
        self.props_from_child = props_from_child

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return str(self.__str__())
