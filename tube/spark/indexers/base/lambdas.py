import ast
import json


def extract_metadata(str_value):
    '''
    Get all fields in _props (strs[3] in hadoop file) and node_id field (strs[4])
    :param str_value:
    :return:
    '''
    str_value = str_value.replace("'", "##")
    str_value = str_value.replace('\\""', "##")
    strs = ast.literal_eval(str_value.replace('""', "'"))
    props = json.loads(strs[3].replace("'", '"').replace("##", "'"), strict=False)
    return tuple([strs[4], props])


def extract_link(str_value):
    strs = ast.literal_eval(str_value)
    return (strs[4], strs[5])


def extract_link_reverse(str_value):
    strs = ast.literal_eval(str_value)
    return (strs[5], strs[4])


def flatten_files_to_lists(pair):
    f, text = pair
    return [line for line in text.splitlines()]


def merge_dictionary(d1, d2):
    d0 = d1.copy()
    d0.update(d2)
    return d0


def swap_key_value(df):
    return df.map(lambda x: (x[1], x[0]))


def get_props(names, values):
    return lambda x: {names[src]: values[src][v] if src in values and v in values[src] else v
                      for (src, v) in x.items() if src in names.keys()}


def get_props_empty_values(props):
    return {k.name: None for k in props}


def merge_and_fill_empty_props(item, props):
    if item[1] is None and item[0] is None:
        return {}
    if item[0] is None:
        return item[1]
    if item[1] is None:
        return merge_dictionary(item[0], get_props_empty_values(props))
    return merge_dictionary(item[0], item[1])


def sort_by_field(x, field, reversed):
    if not reversed:
        return sorted(x, key=lambda k: k[field])
    return sorted(x, key=lambda k: k[field], reverse=reversed)
