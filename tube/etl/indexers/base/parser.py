from ..base.prop import PropFactory
from tube.utils.dd import get_properties_types


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
        PropFactory.adding_prop(self.doc_type, '{}_id'.format(self.doc_type), '', [], None, (str,))

    def get_types(self):
        types = self.select_widest_types({p.name: p.type
                                          for p in PropFactory.get_prop_by_doc_name(self.doc_type).values()
                                          if p.type is not None})
        return types

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

    def get_prop_type(self, fn, src, node_label=None):
        if fn is not None:
            if fn in ['count', 'sum', 'min', 'max']:
                return (float, )
            elif fn in ['set', 'list']:
                return (str, )
            return (str, )
        else:
            if node_label is None:
                raise Exception('An index property must have either path or fn property')
            if src == 'id':
                return (str, )
            a = get_properties_types(self.model, node_label)
            return a.get(src)

    @staticmethod
    def get_src_name(props):
        lst = [tuple(p.split(':')) if ':' in p else tuple([p, p]) for p in props]
        return lst

    def create_prop_from_json(self, doc_name, p, node_label):
        value_mappings = p.get('value_mappings', [])
        src = p['src'] if 'src' in p else p['name']
        fn = p.get('fn')

        prop_type = self.get_prop_type(fn, src, node_label)
        prop = PropFactory.adding_prop(doc_name, p['name'], src, value_mappings, fn, prop_type)
        return prop

    def create_props_from_json(self, doc_name, props_in_json, node_label):
        res = []
        for p in props_in_json:
            res.append(self.create_prop_from_json(doc_name, p, node_label))
        return res
