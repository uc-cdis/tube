import ast
import json


def extract_metadata(str_value):
    '''
    Get all fields in _props (strs[3] in hadoop file) and node_id field (strs[4])
    :param str_value:
    :return:
    '''
    str_value = str_value.replace("'", "##")
    strs = ast.literal_eval(str_value.replace('""', "'"))
    props = json.loads(strs[3].replace("'", '"').replace("##", "'"))
    return tuple([strs[4], props])


def extract_link(str_value):
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


def get_fields(fields):
    return lambda x: {k: v for (k, v) in x.items() if k in fields}


def get_fields_empty_values(fields):
    return {k: None for k in fields}


def merge_and_fill_empty_fields(item, fields):
    if item[1] is None and item[0] is None:
        return {}
    if item[0] is None:
        return item[1]
    if item[1] is None:
        return merge_dictionary(item[0], get_fields_empty_values(fields))
    return merge_dictionary(item[0], item[1])
