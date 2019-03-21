from tube.etl.indexers.base.prop import PropFactory


class SpecialRoot():
    def __init__(self, name, root, fn):
        self.name = name
        self.root = root
        self.fn = fn

    def __key__(self):
        return self.name

    def __hash__(self):
        return hash(self.__key__())

    def __repr__(self):
        return self.name


class SpecialNode():
    def __init__(self, name, tbl, edge_up_tbl, props):
        self.name = name
        self.tbl = tbl
        self.edge_up_tbl = edge_up_tbl
        self.props = PropFactory.create_props_from_json([{'name': p, 'src': p} for p in props])
        self.child = None
