from tube.utils import object_to_string


class BaseNode(object):
    def __init__(self):
        self.children = set([])
        self.no_children_to_map = 0

    def add_child(self, node):
        self.children.add(node)
        self.no_children_to_map = len(self.children)

    def __str__(self):
        return object_to_string(self)
