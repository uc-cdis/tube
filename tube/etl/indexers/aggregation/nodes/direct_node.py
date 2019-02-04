from tube.utils.dd import object_to_string
from tube.etl.indexers.base.prop import PropFactory


class DirectNode(object):
    def __init__(self, name, edge, props_in_json, sorted_by=None, desc_order=False, props_from_child=True):
        self.tbl_name = name
        self.edge = edge
        self.props = PropFactory.create_props_from_json(props_in_json)
        if sorted_by is not None:
            self.sorted_by = PropFactory.add_sorting_field_to_props(self.props, sorted_by)
        else:
            self.sorted_by = None
        self.desc_order = desc_order
        self.props_from_child = props_from_child

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return str(self.__str__())
