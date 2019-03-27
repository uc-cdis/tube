from ..base.prop import PropFactory


class Parser(object):
    """
    The main entry point into the index export process for the mutation indices
    """
    def __init__(self, mapping, model):
        self.mapping = mapping
        self.model = model
        self.name = mapping['name']
        self.root = mapping['root']
        self.doc_type = mapping['doc_type']
        self.joining_nodes = []
        PropFactory.adding_prop(self.doc_type, '{}_id'.format(self.doc_type), '', [])

    def get_types(self):
        pass

    def select_widest_types(self, types):
        for k, v in types.items():
            types[k] = self.select_widest_type(v)
        return types

    def select_widest_type(self, types):
        if str in types:
            return str
        elif float in types:
            return float
        elif long in types:
            return long
        elif int in types:
            return int
        else:
            return str

    def get_key_prop(self):
        return PropFactory.get_prop_by_name(self.doc_type, '{}_id'.format(self.doc_type))

    def get_prop_by_name(self, name):
        return PropFactory.get_prop_by_name(self.doc_type, name)
