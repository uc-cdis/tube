import os
import ast
import json
from tube.utils import make_sure_hdfs_path_exist


def extract_metadata(str_value):
    strs = ast.literal_eval(str_value.replace('""', "'"))
    props = json.loads(strs[3].replace("'", '"'))
    props['id'] = strs[4]
    return props


def extract_link(str_value):
    strs = ast.literal_eval(str_value)
    return (strs[5], strs[4])


def flatten_files_to_lists(pair):
    f, text = pair
    return [line for line in text.splitlines()]


def get_aggregation_func_by_name(func_name, is_merging=False):
    if func_name == 'count':
        if is_merging:
            return rdd_sum
        return rdd_count
    if func_name == 'sum':
        return rdd_sum


def rdd_count(x, y):
    return x + 1


def rdd_sum(x, y):
    return x + y


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


def create_zero_frame(x):
    r = []
    for i in x[1]:
        r.append((i[0], i[1], 0))
    return tuple(r)


def create_output_frame(x):
    r = []
    for i in x:
        r.append((i[1], i[2]))
    return tuple(r)


def intermediate_frame(output_name):
    return lambda x: (('sum', output_name, x),)


class Gen3Translator(object):
    """
    The main entry point into the index export process for the mutation indices
    """
    def __init__(self, sc, parser, config):
        self.sc = sc
        self.parser = parser
        self.config = config
        self.logger = config.logger
        self.hdfs_path = make_sure_hdfs_path_exist(config.HDFS_DIR, sc)

    def translate_table(self, table_name):
        df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(flatten_files_to_lists)
        return df.map(extract_metadata)

    def translate_edge(self, table_name):
        df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(flatten_files_to_lists)
        return df.map(extract_link)

    def aggregate_intermediate_data_frame(self, child_df, edge_df):
        temp_df = edge_df.join(child_df).map(lambda x: (x[1][0], x[1][1]))
        return temp_df.aggregateByKey(create_zero_frame(temp_df.first()),
                                      seq_aggregate_with_reducer,
                                      merge_aggregate_with_reducer)

    def run_etl(self):
        aggregated_dfs = {}
        for n in self.parser.nodes:
            df = None
            for child in n.children:
                if child.no_children_to_map == 0:
                    edge_df = self.translate_edge(child.edge_up_tbl)
                    count_df = edge_df.groupByKey().mapValues(len).mapValues(intermediate_frame(child.reducer.output))
                    df = count_df if df is None else df.join(count_df).mapValues(lambda x: x[0] + x[1])

                    df = df if child.__key__() not in aggregated_dfs \
                        else df.join(self.aggregate_intermediate_data_frame(aggregated_dfs[child.__key__()],
                                                                            edge_df.map(lambda x: (x[1], x[0]))))\
                        .mapValues(lambda x: x[0] + x[1])
                    n.no_children_to_map -= 1
            aggregated_dfs[n.__key__()] = df
        root_df = aggregated_dfs[self.parser.root].mapValues(lambda x: create_output_frame(x))
        root_df.saveAsTextFile(os.path.join(self.hdfs_path, 'export', 'result'))
