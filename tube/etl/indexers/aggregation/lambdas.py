from tube.etl.indexers.base.lambdas import (
    get_aggregation_func_by_name,
    get_single_frame_zero_by_func,
    get_single_frame_value,
)
import builtins


def get_frame_zero(reducers):
    return tuple(
        [
            get_single_frame_zero_by_func(rd.fn, rd.prop.id)
            for rd in reducers
            if not rd.done
        ]
    )


def get_normal_frame(reducers):
    """
    Create a tuple for every value, this tuple include (func-name, output_field_name, value).
    It helps spark workers know directly what to do when reading the dataframe
    :param reducers:
    :return:
    """
    return lambda x: tuple(
        [
            (rd.fn, rd.prop.id, get_single_frame_value(rd.fn, x.get(rd.prop.id)))
            for rd in reducers
            if not rd.done
        ]
    )


def get_normal_dict_item():
    """
    Create a tuple for every value, this tuple include (func-name, output_field_name, value).
    It helps spark workers know directly what to do when reading the dataframe
    :param reducers:
    :return:
    """
    return lambda x: {k: [v] if not isinstance(v, list) else v for k, v in x.items()}


def seq_or_merge_aggregate_dictionary_with_set_fn(x, y):
    """
    Sequencing function that works with the dataframe created by get_normal_frame
    :param x:
    :param y:
    :return:
    """
    res = {}
    for k, v in x.items():
        if v is None and y.get(k) is None:
            res[k] = []
        elif v is None and y.get(k) is not None:
            res[k] = y.get(k)
        elif v is not None and y.get(k) is None:
            res[k] = v
        else:
            res[k] = list(set(v) | set(y.get(k)))
    return res


def get_dict_zero_with_set_fn(props):
    return {p.id: [] for p in props}


def intermediate_frame(prop):
    # Generalize later base on the input
    return lambda x: (("sum", prop.id, x),)


def sliding(rdd, n, fn1, fn2):
    def gen_window(xi, n):
        k, v = xi
        x = tuple(v)
        return [((k, x[0] - offset), (x[0], x[1])) for offset in range(n)]

    return (
        rdd.flatMap(lambda xi: gen_window(xi, n))
        .groupByKey()
        .mapValues(lambda vals: [x for (i, x) in sorted(vals)])
        .sortByKey()
        .filter(lambda x: len(x[1]) == n)
        .mapValues(lambda x: getattr(builtins, fn1)(x))
        .map(lambda x: (x[0][0], x[1]))
        .groupByKey()
        .mapValues(lambda vals: getattr(builtins, fn2)(vals))
    )
