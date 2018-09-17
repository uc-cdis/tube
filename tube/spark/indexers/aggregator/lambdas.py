def get_aggregation_func_by_name(func_name, is_merging=False):
    if func_name == 'count':
        if is_merging:
            return lambda x, y: x + y
        return lambda x, y: x + 1
    if func_name == 'sum':
        return lambda x, y: x + y


def seq_aggregate_with_reducer(x, y):
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
