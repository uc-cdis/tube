import ast
import json
import collections
import functools
import pyspark.sql.functions as f
import pyspark.sql.types as t
from tube.utils.general import get_node_id_name


def extract_metadata(str_value):
    """
    Get all fields in _props (strs[3] in hadoop file) and node_id field (strs[4])
    :param str_value:
    :return:
    """
    origin_value = str_value
    str_value = str_value.replace("'", "###")
    str_value = str_value.replace('\\""', "##")
    strs = ast.literal_eval(str_value.replace('""', "'"))
    try:
        props = json.loads(
            strs[3].replace("'", '"').replace("###", "'").replace("##", '\\"'),
            strict=False,
        )
    except Exception as ex:
        raise Exception(
            "ERROR IN SPARK: origin: {}, after replacing: {}".format(
                origin_value, str_value
            )
        )
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


def merge_dictionary(d1, d2, to_tuple=False):
    d0 = d1.copy()
    if d2 is not None and len(d2) > 0:
        d0.update(d2)
    return (
        d0
        if not to_tuple
        else tuple(
            [
                (k, v) if type(v) != list else (k, tuple(v))
                for (k, v) in list(d0.items())
            ]
        )
    )


def swap_key_value(df):
    return df.map(lambda x: (x[1], x[0]))


def get_props(names, values):
    return lambda x: {
        p_id: values[src][p_id][v]
        if isinstance(v, collections.Hashable)
        and src in values
        and v in values[src][p_id]
        else v
        for (src, v) in list(x.items())
        if src in list(names.keys())
        for p_id in names[src]
    }


def get_props_empty_values(props):
    return {k.id: None for k in props}


def merge_data_frames(x):
    if x[0] is None and x[1] is None:
        return tuple([])
    if x[0] is None:
        return x[1]
    if x[1] is None:
        return x[0]
    return tuple(list(set(x[0]) | set(x[1])))


def get_number(num):
    if num is None:
        return None
    try:
        return int(num)
    except ValueError as e:
        return num


def make_key_from_property(x1, prop_name):
    key = x1.pop(prop_name, None)
    x0 = key
    return (x0, x1)


def use_property_as_key(x0, x1, prop_name, new_prop_name):
    key = x1.pop(prop_name, None)
    x1[new_prop_name] = x0
    x0 = key
    return (x0, x1)


def swap_property_as_key(df, prop_name, new_prop_name):
    return df.map(lambda x: use_property_as_key(x[0], x[1], prop_name, new_prop_name))


def merge_and_fill_empty_props(item, props, to_tuple=False):
    if item[1] is None and item[0] is None:
        return {} if not to_tuple else tuple([])
    if item[0] is None:
        return (
            item[1]
            if not to_tuple
            else tuple(
                [
                    (k, v) if type(v) != list else (k, tuple(v))
                    for (k, v) in list(item[1].items())
                ]
            )
        )
    if item[1] is None:
        return merge_dictionary(item[0], get_props_empty_values(props), to_tuple)
    return merge_dictionary(item[0], item[1], to_tuple)


def sort_by_field(x, field, reversed):
    if not reversed:
        return sorted(x, key=lambda k: k[field])
    return sorted(x, key=lambda k: k[field], reverse=reversed)


def union_sets(x, y):
    if x is None and y is None:
        return []
    elif x is None and y is not None:
        return y
    elif x is not None and y is None:
        return x
    else:
        return list(set(x) | set(y))


def extend_list(x, y):
    if x is None and y is None:
        return []
    elif x is None and y is not None:
        return y
    elif x is not None and y is None:
        return x
    else:
        x.extend(y)
        return x


def get_aggregation_func_by_name(func_name, is_merging=False):
    if func_name == "count":
        if is_merging:
            return lambda x, y: x + y
        return lambda x, y: x + 1
    if func_name == "sum":
        return lambda x, y: x + y
    if func_name == "set":
        return lambda x, y: union_sets(x, y)
    if func_name == "list":
        return lambda x, y: extend_list(x, y)
    if func_name == "min":
        return (
            lambda x, y: None
            if x is None and y is None
            else min([i for i in [x, y] if i is not None])
        )
    if func_name == "max":
        return (
            lambda x, y: None
            if x is None and y is None
            else max([i for i in [x, y] if i is not None])
        )


def get_single_frame_zero_by_func(func_name, output_name):
    if func_name in ["set", "list"]:
        return (func_name, output_name, [])
    if func_name == "count" or func_name == "sum":
        return (func_name, output_name, 0)
    if func_name in ["min", "max"]:
        return (func_name, output_name, None)
    return (func_name, output_name, "")


def get_single_frame_value(func_name, value):
    if func_name in ["set", "list"]:
        if value is None:
            return []
        return [value] if not isinstance(value, list) else value
    if func_name == "count":
        return 1 if value is None else value
    if func_name == "sum":
        return 0 if value is None else value
    return value


def f_concat_udf(val):
    return functools.reduce(lambda x, y: x + y, val)


f_collect_list_udf = f.udf(f_concat_udf, t.ArrayType(t.StringType()))


def f_set_union_udf(val):
    return functools.reduce(lambda x, y: list(set(x) | set(y)), val)


f_collect_set_udf = f.udf(f_set_union_udf, t.ArrayType(t.StringType()))
