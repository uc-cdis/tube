from tube.etl.indexers.base.prop import PropFactory


class ParentChain:
    """
    Special root is the abstract node give the access to the aggregation path of a special_prop in etlMapping
    """

    def __init__(self, head, fn):
        self.head = head  # first ParentNode in the chain of nodes (like a link-list) helping to getting props
        # from parent nodes in the dictionary
        self.fn = fn  # name of the function perform in this aggregation node

    def __key__(self):
        return self.head.name

    def __hash__(self):
        return hash(self.__key__())

    def __repr__(self):
        return self.head.name


class ParentNode:
    """
    Special Node is a node in a chain of aggregation path that will be follow by the special aggregation
    """

    def __init__(self, name, tbl, edge_up_tbl, props):
        self.name = name
        self.tbl_name = tbl
        self.edge_up_tbl = edge_up_tbl
        self.props = props  # PropFactory.create_props_from_json(doc_name,
        self.child = None
