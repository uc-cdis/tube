from tube.utils.dd import object_to_string


class BaseNode:
    def __init__(self):
        self.queries = []

    def add_query(self, query):
        self.queries.append(query)


class BaseCompoundNode(BaseNode):
    def __init__(self):
        super(BaseCompoundNode, self).__init__()
        self.children = set([])
        self.no_children_to_map = 0

    def add_child(self, node):
        self.children.add(node)
        self.no_children_to_map = len(self.children)

    def __str__(self):
        return object_to_string(self)

# SELECT a, b, count(a), sum(b)
# FROM table_t t
# WHERE t.id = something
