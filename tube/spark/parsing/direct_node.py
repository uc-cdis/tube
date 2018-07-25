from tube.utils import object_to_string


class DirectNode(object):
    def __init__(self, name, edge, fields):
        self.tbl_name = name
        self.edge = edge
        self.fields = fields

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return str(self.__str__())
