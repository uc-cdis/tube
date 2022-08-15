from tube.utils.dd import object_to_string


class PropFactory(object):
    list_props = []
    list_additional_props = []
    prop_by_names = {}
    additional_prop_by_names = {}

    @staticmethod
    def get_length():
        return len(PropFactory.list_props)

    @staticmethod
    def get_additional_length():
        return len(PropFactory.list_additional_props)

    @staticmethod
    def create_value_mappings(value_mappings_in_json):
        res = []
        if isinstance(value_mappings_in_json, list):
            for item in value_mappings_in_json:
                k = list(item.keys())[0]
                res.append(ValueMapping(k, item[k]))
        elif isinstance(value_mappings_in_json, dict):
            for k, v in value_mappings_in_json:
                res.append(ValueMapping(k, v))
        return res

    @staticmethod
    def adding_prop(
        doc_name,
        name,
        src,
        value_mappings,
        src_node=None,
        src_index=None,
        fn=None,
        prop_type=None,
        is_additional=False,
    ):
        if doc_name not in PropFactory.prop_by_names:
            PropFactory.prop_by_names[doc_name] = {}
        prop = PropFactory.get_prop_by_name(doc_name, name)
        if prop is None:
            prop = Prop(
                PropFactory.get_length(),
                name,
                src,
                PropFactory.create_value_mappings(value_mappings),
                src_node,
                src_index,
                fn,
                prop_type,
                is_additional=is_additional,
            )
            PropFactory.list_props.append(prop)
            PropFactory.prop_by_names.get(doc_name)[name] = prop
        return prop

    @staticmethod
    def add_additional_prop(doc_name, name, prop_type=None):
        if doc_name not in PropFactory.additional_prop_by_names:
            PropFactory.additional_prop_by_names[doc_name] = {}
        prop = PropFactory.get_additional_prop_by_name(doc_name, name)
        if prop is None:
            prop = Prop(
                PropFactory.get_additional_length(),
                name,
                None,
                [],
                None,
                None,
                None,
                prop_type,
                is_additional=True,
            )
            PropFactory.list_additional_props.append(prop)
            PropFactory.additional_prop_by_names.get(doc_name)[name] = prop
        return prop

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
    def get_additional_prop_by_name(doc_name, name):
        sub_dict_props = PropFactory.get_additional_prop_by_doc_name(doc_name)
        if sub_dict_props is None:
            return None
        return sub_dict_props.get(name)

    @staticmethod
    def get_prop_by_doc_name(doc_name):
        return PropFactory.prop_by_names.get(doc_name)

    @staticmethod
    def get_additional_prop_by_doc_name(doc_name):
        return PropFactory.additional_prop_by_names.get(doc_name)

    @staticmethod
    def get_prop_by_json(doc_name, p):
        return PropFactory.get_prop_by_name(doc_name, p["name"])

    @staticmethod
    def has_prop_in_doc_name(doc_name, name):
        return (
            doc_name in PropFactory.prop_by_names
            and name in PropFactory.prop_by_names.get(doc_name)
        )


class ValueMapping(object):
    def __init__(self, original, final):
        self.original = original
        self.final = final


class Prop(object):
    def __init__(
        self,
        id,
        name,
        src,
        value_mappings,
        src_node,
        src_index=None,
        fn=None,
        prop_type=None,
        is_additional=False,
    ):
        self.id = id
        self.name = name
        self.src = src
        self.src_node = src_node
        self.src_index = src_index
        self.value_mappings = [] if value_mappings is None else value_mappings
        self.fn = fn
        self.type = prop_type
        self.is_additional = is_additional

    def __hash__(self):
        return self.id

    def __str__(self):
        return object_to_string(self)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.id == other.id

    def __ne__(self, other):
        return self.id != other.id

    def update_type(self, prop_type):
        self.type = prop_type
