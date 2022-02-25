from tube.etl.indexers.base.node import BaseCompoundNode
from tube.etl.indexers.aggregation.nodes.aggregated_node import Reducer


class JoiningNode(BaseCompoundNode):
    def __init__(self, props, json_join):
        super(JoiningNode, self).__init__()
        self.joining_index = json_join["index"]
        self.joining_fields = [f.strip() for f in json_join["join_on"].split(",")]
        self.getting_fields = [Reducer(p, p.fn) for p in props]

    def __key__(self):
        return self.joining_index

    def __hash__(self):
        return hash(self.__key__())

    def __repr__(self):
        return "({}; {})".format(str(self.__key__()), self.getting_fields)
