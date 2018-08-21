import os
import ast
import json
from tube.utils import make_sure_hdfs_path_exist


def extract_metadata(str_value):
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
    return res


def intermediate_frame(output_name):
    return lambda x: (('sum', output_name, x),)


def intermediate_zero_frame(output_name):
    return lambda x: (('sum', output_name, 0),)


def swap_key_value(df):
    return df.map(lambda x: (x[1], x[0]))


def get_fields(fields):
    return lambda x: {k: v for (k, v) in x.items() if k in fields}


def merge_dictionary(d1, d2):
    d0 = d1.copy()
    d0.update(d2)
    return d0


class Gen3Translator(object):
    """
    The main entry point into the index export process for the mutation indices
    """
    def __init__(self, sc, parser, writer, config):
        self.sc = sc
        self.parser = parser
        self.config = config
        self.logger = config.logger
        self.writer = writer
        self.hdfs_path = make_sure_hdfs_path_exist(config.HDFS_DIR, sc)

    def translate_table(self, table_name, get_zero_frame=None, fields=None):
        df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(flatten_files_to_lists)
        df = df.map(extract_metadata)

        if get_zero_frame:
            if df.isEmpty():
                df = self.sc.parallelize([('__BLANK_ID__', '__BLANK_VALUE__')])  # to create the frame for empty node
            return df.mapValues(lambda x: [])
        if fields is not None:
            return df.mapValues(get_fields(fields))
        return df

    def translate_edge(self, table_name):
        df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(flatten_files_to_lists)
        return df.map(extract_link)

    def aggregate_intermediate_data_frame(self, child_df, edge_df):
        frame_zero = tuple([(i[0], i[1], 0) for i in child_df.first()[1]])
        temp_df = edge_df.leftOuterJoin(child_df).map(lambda x: (x[1][0], x[1][1]))\
            .mapValues(lambda x: x if x is not None else frame_zero)
        return temp_df.aggregateByKey(frame_zero,
                                      seq_aggregate_with_reducer,
                                      merge_aggregate_with_reducer)

    def aggregate_nested_properties(self):
        aggregated_dfs = {}
        for n in self.parser.aggregated_nodes:
            df = None
            key_df = self.translate_table(n.tbl_name, get_zero_frame=True)
            for child in n.children:
                if child.no_children_to_map == 0:
                    edge_df = key_df.leftOuterJoin(self.translate_edge(child.edge_up_tbl)).mapValues(lambda x: x[1])
                    if child.reducer is None:
                        count_df = edge_df.groupByKey().mapValues(lambda x: ())
                    else:
                        count_df = edge_df.groupByKey().mapValues(lambda x: len([i for i in x if i is not None]))\
                            .mapValues(intermediate_frame(child.reducer.output))

                    df = count_df if df is None else df.leftOuterJoin(count_df).mapValues(lambda x: x[0] + x[1])
                    # print(df.collect())
                    df = df if child.__key__() not in aggregated_dfs \
                        else df.leftOuterJoin(self.aggregate_intermediate_data_frame(aggregated_dfs[child.__key__()],
                                                                                     swap_key_value(edge_df))) \
                        .mapValues(lambda x: x[0] + x[1])
                    n.no_children_to_map -= 1
                else:
                    df = key_df
            aggregated_dfs[n.__key__()] = df
        return aggregated_dfs[self.parser.root].mapValues(lambda x: {x1: x2 for (x0, x1, x2) in x})

    def get_direct_children(self, root_df):
        for n in self.parser.flatten_props:
            edge_df = self.translate_edge(n.edge)
            reversed_df = swap_key_value(edge_df)
            child_df = self.translate_table(n.tbl_name, fields=n.fields)
            child_df = reversed_df.join(child_df).map(lambda x: tuple([x[1][0], x[1][1]]))
            root_df = root_df.join(child_df).mapValues(lambda x: merge_dictionary(x[0], x[1]))
        return root_df

    def run_etl(self):
        root_df = self.translate_table(self.parser.root_table, fields=self.parser.root_fields)
        root_df = self.get_direct_children(root_df)
        root_df = root_df.join(self.aggregate_nested_properties()).mapValues(lambda x: merge_dictionary(x[0], x[1]))
        self.writer.write_df(root_df, self.parser.root)
