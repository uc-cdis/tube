from tube.etl.indexers.base.node import BaseNode
from tube.etl.indexers.aggregation.nodes.aggregated_node import Reducer


class JoiningNode(BaseNode):
    def __init__(self, props, json_join):
        super(JoiningNode, self).__init__()
        self.joining_index = json_join["index"]
        self.joining_field = json_join["join_on"]
        self.getting_fields = [Reducer(p, p.fn) for p in props]

    def __key__(self):
        return self.joining_index

    def __hash__(self):
        return hash(self.__key__())

    def __repr__(self):
        return "({}; {})".format(str(self.__key__()), self.getting_fields)
