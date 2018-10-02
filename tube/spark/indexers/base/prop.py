from tube.utils import object_to_string


class PropFactory(object):
    @staticmethod
    def create_value_mappings(value_mappings_in_json):
        res = []
        for item in value_mappings_in_json:
            k = item.keys()[0]
            res.append(ValueMapping(k, item[k]))
        return res

    @staticmethod
    def create_props_from_json(props_in_json):
        res = []
        for p in props_in_json:
            value_mappings = p['value_mappings'] if 'value_mappings' in p else []
            src = p['src'] if 'src' in p else p['name']
            res.append(Prop(p['name'], src, PropFactory.create_value_mappings(value_mappings)))
        return res


class ValueMapping(object):
    def __init__(self, original, final):
        self.original = original
        self.final = final


class Prop(object):
    def __init__(self, name, src, value_mappings):
        self.name = name
        self.src = src
        self.value_mappings = [] if value_mappings is None else value_mappings

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return self.__str__()
