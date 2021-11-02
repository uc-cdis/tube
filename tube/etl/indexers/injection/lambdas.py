from tube.etl.indexers.base.lambdas import (
    get_aggregation_func_by_name,
    get_single_frame_zero_by_func,
    get_single_frame_value,
)


def get_frame_zero(props):
    return tuple([get_single_frame_zero_by_func(p.fn, p.id) for p in props])


def get_normal_frame(props):
    """
    Create a tuple for every value, this tuple include (func-name, output_field_name, value).
    It helps spark workers know directly what to do when reading the dataframe
    :param reducers:
    :return:
    """
    return lambda x: tuple(
        [(p.fn, p.id, get_single_frame_value(p.fn, x.get(p.id))) for p in props]
    )


def remove_props_from_tuple(x, props):
    for p in props:
        x.pop(p.id, None)
    return tuple([(k, v) for (k, v) in list(x.items())])


def get_props_to_tuple(x, props):
    get_props = []
    for p in props:
        get_props.append(tuple([p.fn, p.id, get_single_frame_value(p.fn, x.get(p.id))]))
    return tuple(get_props)


def construct_project_id(x, props, output):
    return {output: "-".join([x.pop(p.id, "") for p in props])}
