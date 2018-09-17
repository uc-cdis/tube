import os
from tube.utils import make_sure_hdfs_path_exist
from .base.lambdas import extract_metadata, extract_link, \
    flatten_files_to_lists, merge_dictionary, merge_and_fill_empty_fields, \
    swap_key_value, get_fields, get_fields_empty_values
from .aggregator.lambdas import intermediate_frame, merge_aggregate_with_reducer, \
    seq_aggregate_with_reducer
from .collector.nodes.collecting_node import LeafNode


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
            if df.isEmpty():
                return df.mapValues(get_fields_empty_values(fields))
            return df.mapValues(get_fields(fields))
        return df

    def translate_edge(self, table_name):
        df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(flatten_files_to_lists)
        return df.map(extract_link)

    def aggregate_intermediate_data_frame(self, child_df, edge_df):
        frame_zero = tuple([(i[0], i[1], 0) for i in child_df.first()[1]])
        temp_df = edge_df.leftOuterJoin(child_df).map(lambda x: (x[1][0], x[1][1])) \
            .mapValues(lambda x: x if x is not None else frame_zero)
        return temp_df.aggregateByKey(frame_zero,
                                      seq_aggregate_with_reducer,
                                      merge_aggregate_with_reducer)

    def aggregate_nested_properties(self):
        """
        Create aggregated nodes from the deepest level of the aggregation tree.
        A map/reduce step will be performed for an aggregated node when the map/reduce step in all its children nodes
        were done.
        :return:
            A dataframe including all the aggregated fields
        """
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
                        count_df = edge_df.groupByKey().mapValues(lambda x: len([i for i in x if i is not None])) \
                            .mapValues(intermediate_frame(child.reducer.output))

                    df = count_df if df is None else df.leftOuterJoin(count_df).mapValues(lambda x: x[0] + x[1])
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
            # edge_df = root_df.leftOuterJoin(self.translate_edge(n.edge)).mapValues(lambda x: x[1])
            edge_df = self.translate_edge(n.edge)
            reversed_df = swap_key_value(edge_df)
            child_df = self.translate_table(n.tbl_name, fields=n.fields)
            child_df = reversed_df.join(child_df).map(lambda x: tuple([x[1][0], x[1][1]]))
            root_df = root_df.leftOuterJoin(child_df).mapValues(lambda x: merge_and_fill_empty_fields(x, n.fields))
        return root_df

    def collect_child(self, child, edge_df, collected_dfs):
        if edge_df.isEmpty():
            child.is_empty = True
            return
        child_df = collected_dfs[child.name] if child.name in collected_dfs else \
            self.translate_table(child.tbl_name,
                                 self.parser.common_targeting_fields
                                 if type(child) is LeafNode
                                 else [])
        child_df = child_df.leftOuterJoin(edge_df)
        child.no_parent_to_map -= 1
        if child.no_parent_to_map == 0:
            child.done = True
        if child.name not in collected_dfs:
            collected_dfs[child.name] = child_df

    def merge_roots_to_children(self):
        collected_dfs = {}
        for root in self.parser.roots:
            df = self.translate_table(root.tbl_name)
            for child in root.children:
                edge_df = df.rightOuterJoin(self.translate_edge(child.edge_up_tbl))\
                    .map(lambda x: (x[1][1], ({'{}_id'.format(root.name): x[0]},) + x[1][0]))\
                    .mapValues(lambda x: merge_and_fill_empty_fields(x, []))
                self.collect_child(child, edge_df, collected_dfs)
        return collected_dfs

    def merge_collectors(self, collected_dfs):
        for collector in self.parser.collectors:
            if collector.no_parent_to_map == 0:
                df = collected_dfs[collector.name]
                for child in collector.children:
                    edge_df = df.rightOuterJoin(self.translate_edge(child.edge_up_tbl)) \
                        .map(lambda x: (x[1][1], x[1][0]))
                    self.collect_child(child, edge_df, collected_dfs)

    def get_leaves(self, collected_dfs):
        for leaf in self.parser.leaves:
            if not leaf.done:
                for parent in leaf.parents:
                    if parent.name not in collected_dfs.keys():
                        continue
                    df = collected_dfs[parent.name]
                    edge_df = df.rightOuterJoin(self.translate_edge(leaf.edge_up_tbl))\
                        .map(lambda x: (x[1][1], x[1][0]))
                    self.collect_child(leaf, edge_df, collected_dfs)

    def collect_files(self):
        collected_dfs = self.merge_roots_to_children()
        self.merge_collectors(collected_dfs)
        self.get_leaves(collected_dfs)

    def run_etl(self):
        root_df = self.translate_table(self.parser.root_table, fields=self.parser.root_fields)
        root_df = self.get_direct_children(root_df)
        root_df = root_df.join(self.aggregate_nested_properties()).mapValues(lambda x: merge_dictionary(x[0], x[1]))
        self.writer.write_df(root_df, self.parser.root, self.parser.types)
