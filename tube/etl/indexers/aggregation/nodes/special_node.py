from tube.etl.indexers.base.prop import PropFactory


class SpecialChain:
    """
    Special root is the abstract node give the access to the aggregation path of a special_prop in etlMapping
    """

    def __init__(self, doc_name, name, head, fn):
        self.name = name  # name of the prop create by this special function
        self.head = head  # first SpecialNode in the chain of nodes (like a link-list) related to the special function
        self.fn = fn  # name of the function perform in this special aggregation node
        PropFactory.adding_prop(
            doc_name,
            name,
            "",
            [],
            src_node=None,
            src_index=None,
            fn="",
            prop_type=(float,),
        )

    def __key__(self):
        return self.name

    def __hash__(self):
        return hash(self.__key__())

    def __repr__(self):
        return self.name


class SpecialNode:
    """
    Special Node is a node in a chain of aggregation path that will be follow by the special aggregation
    """

    def __init__(self, name, tbl, edge_up_tbl, props):
        self.name = name
        self.tbl = tbl
        self.edge_up_tbl = edge_up_tbl
        self.props = props
        self.child = None
