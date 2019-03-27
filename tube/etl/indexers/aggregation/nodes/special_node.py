from tube.etl.indexers.base.prop import PropFactory


class SpecialRoot():
    """
    Special root is the abstract node give the access to the aggregation path of a special_prop in etlMapping
    """
    def __init__(self, doc_name, name, head, fn):
        self.name = name  # name of the prop create by this special function
        self.head = head  # first SpecialNode in the chain of nodes (like a link-list) related to the special function
        self.fn = fn  # name of the function perform in this special aggregation node
        PropFactory.adding_prop(doc_name, name, '', [], '')

    def __key__(self):
        return self.name

    def __hash__(self):
        return hash(self.__key__())

    def __repr__(self):
        return self.name


class SpecialNode():
    """
    Special Node is a node in a chain of aggregation path that will be follow by the special aggregation
    """
    def __init__(self, doc_name, name, tbl, edge_up_tbl, props):
        self.name = name
        self.tbl = tbl
        self.edge_up_tbl = edge_up_tbl
        self.props = PropFactory.create_props_from_json(doc_name, [{'name': p, 'src': p} for p in props])
        self.child = None
