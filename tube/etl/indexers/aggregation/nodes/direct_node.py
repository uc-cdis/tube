from tube.utils.dd import object_to_string
from tube.etl.indexers.base.prop import PropFactory


class DirectNode(object):
    def __init__(self, doc_name, name, edge, props_in_json, sorted_by=None, desc_order=False, props_from_child=True):
        self.tbl_name = name
        self.edge = edge
        self.props = PropFactory.create_props_from_json(doc_name, props_in_json)
        self.sorted_by = sorted_by
        self.desc_order = desc_order
        self.props_from_child = props_from_child

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return str(self.__str__())
