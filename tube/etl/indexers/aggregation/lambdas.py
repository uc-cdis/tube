def union_sets(x, y):
    if x is None and y is None:
        return []
    elif x is None and y is not None:
        return y
    elif x is not None and y is None:
        return x
    else:
        return list(set(x) | set(y))


def get_aggregation_func_by_name(func_name, is_merging=False):
    if func_name == 'count':
        if is_merging:
            return lambda x, y: x + y
        return lambda x, y: x + 1
    if func_name == 'sum':
        return lambda x, y: x + y
    if func_name == 'set':
        return lambda x, y: union_sets(x, y)


def get_single_frame_zero_by_func(func_name, output_name):
    if func_name == 'set':
        return (func_name, output_name, [])
    if func_name == 'count' or func_name == 'sum':
        return (func_name, output_name, 0)
    return (func_name, output_name, '')


def get_frame_zero(reducers):
    return tuple([get_single_frame_zero_by_func(rd.fn, rd.prop.name) for rd in reducers if not rd.done])


def get_single_frame_value(func_name, value):
    if func_name == 'set':
        if value is None:
            return []
        return [value] if type(value) is not list else value
    if func_name == 'count':
        return 1 if value is None else value
    if func_name == 'sum':
        return 0 if value is None else value
    return ''


def get_normal_frame(reducers):
    """
    Create a tuple for every value, this tuple include (func-name, output_field_name, value).
    It helps spark workers know directly what to do when reading the dataframe
    :param reducers:
    :return:
    """
    return lambda x: tuple([(rd.fn, rd.prop.name, get_single_frame_value(rd.fn, x.get(rd.prop.name)))
                            for rd in reducers if not rd.done])


def seq_aggregate_with_reducer(x, y):
    """
    Sequencing function that works with the dataframe created by get_normal_frame
    :param x:
    :param y:
    :return:
    """
    res = []
    for i in range(0, len(x)):
        res.append((x[i][0], x[i][1], get_aggregation_func_by_name(x[i][0])(x[i][2], y[i][2])))
    return tuple(res)


def merge_aggregate_with_reducer(x, y):
    res = []
    for i in range(0, len(x)):
        res.append((x[i][0], x[i][1], get_aggregation_func_by_name(x[i][0], True)(x[i][2], y[i][2])))
    return tuple(res)


def intermediate_frame(output_name):
    # Generalize later base on the input
    return lambda x: (('sum', output_name, x),)
