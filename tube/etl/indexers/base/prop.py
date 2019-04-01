from tube.utils.dd import object_to_string


class PropFactory(object):
    list_props = []
    prop_by_names = {}

    @staticmethod
    def get_length():
        return len(PropFactory.list_props)

    @staticmethod
    def create_value_mappings(value_mappings_in_json):
        res = []
        if type(value_mappings_in_json) is list:
            for item in value_mappings_in_json:
                k = item.keys()[0]
                res.append(ValueMapping(k, item[k]))
        elif type(value_mappings_in_json) is dict:
            for k, v in value_mappings_in_json:
                res.append(ValueMapping(k, v))
        return res

    @staticmethod
    def adding_prop(doc_name, name, src, value_mappings, fn=None, prop_type=None):
        if doc_name not in PropFactory.prop_by_names:
            PropFactory.prop_by_names[doc_name] = {}
        prop = PropFactory.get_prop_by_name(doc_name, name)
        if prop is None:
            prop = Prop(PropFactory.get_length(), name, src,
                        PropFactory.create_value_mappings(value_mappings), fn, prop_type)
            PropFactory.list_props.append(prop)
            PropFactory.prop_by_names.get(doc_name)[name] = prop
        return prop

    @staticmethod
    def create_prop_from_json(doc_name, p):
        value_mappings = p.get('value_mappings', [])
        src = p['src'] if 'src' in p else p['name']
        fn = p.get('fn')
        return PropFactory.adding_prop(doc_name, p['name'], src, value_mappings, fn)

    @staticmethod
    def get_prop_by_id(id):
        return PropFactory.list_props[id]

    @staticmethod
    def get_prop_by_name(doc_name, name):
        sub_dict_props = PropFactory.get_prop_by_doc_name(doc_name)
        if sub_dict_props is None:
            return None
        return sub_dict_props.get(name)

    @staticmethod
    def get_prop_by_doc_name(doc_name):
        return PropFactory.prop_by_names.get(doc_name)

    @staticmethod
    def get_prop_by_json(doc_name, p):
        return PropFactory.get_prop_by_name(doc_name, p['name'])

    @staticmethod
    def create_props_from_json(doc_name, props_in_json):
        res = []
        for p in props_in_json:
            res.append(PropFactory.create_prop_from_json(doc_name, p))
        return res


class ValueMapping(object):
    def __init__(self, original, final):
        self.original = original
        self.final = final


class Prop(object):
    def __init__(self, id, name, src, value_mappings, fn=None, prop_type=None):
        self.id = id
        self.name = name
        self.src = src
        self.value_mappings = [] if value_mappings is None else value_mappings
        self.fn = fn
        self.type = prop_type

    def __hash__(self):
        return self.id

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return self.__str__()

    def update_type(self, prop_type):
        self.type = prop_type
