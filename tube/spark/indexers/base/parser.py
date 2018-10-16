from ..base.prop import Prop


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

    def get_types(self):
        pass

    def get_prop(self, p):
        src = p['src'] if 'src' in p else p['name']
        return Prop(p['name'], src, [])
