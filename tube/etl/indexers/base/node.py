from tube.utils.dd import object_to_string


class BaseNode(object):
    def __init__(self):
        self.children = set([])
        self.no_children_to_map = 0

    def add_child(self, node):
        self.children.add(node)
        self.no_children_to_map = len(self.children)

    def __str__(self):
        return object_to_string(self)


class BaseRootNode(BaseNode):
    def __init__(self, name, tbl_name, props):
        super(BaseRootNode, self).__init__()
        self.name = name
        self.tbl_name = tbl_name
        self.props = props

    def __repr__(self):
        return self.name
