from tube.spark.indexers.base.lambdas import merge_and_fill_empty_props, merge_dictionary, swap_key_value
from tube.spark.indexers.base.translator import Translator as BaseTranslator
from tube.utils import get_node_table_name
from .lambdas import intermediate_frame, merge_aggregate_with_reducer, seq_aggregate_with_reducer
from .parser import Parser


class Translator(BaseTranslator):
    def __init__(self, sc, hdfs_path, writer, mapping, model, dictionary):
        super(Translator, self).__init__(sc, hdfs_path, writer)
        self.parser = Parser(mapping, model, dictionary)

    def aggregate_intermediate_data_frame(self, child_df, edge_df):
        frame_zero = tuple([(i[0], i[1], 0) for i in child_df.first()[1]])
        temp_df = edge_df.leftOuterJoin(child_df).map(lambda x: (x[1][0], x[1][1]))\
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
                        count_df = edge_df.groupByKey().mapValues(lambda x: len([i for i in x if i is not None]))\
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
            edge_df = self.translate_edge(n.edge)
            reversed_df = swap_key_value(edge_df) if n.props_from_child else edge_df
            child_df = self.translate_table(n.tbl_name, props=n.props)
            child_df = reversed_df.join(child_df).map(lambda x: tuple([x[1][0], x[1][1]]))
            root_df = root_df.leftOuterJoin(child_df).mapValues(lambda x: merge_and_fill_empty_props(x, n.props))
        return root_df

    def translate(self):
        root_tbl = get_node_table_name(self.parser.model, self.parser.root)
        root_df = self.translate_table(root_tbl, props=self.parser.props)
        root_df = self.get_direct_children(root_df)
        root_df = root_df.join(self.aggregate_nested_properties()).mapValues(lambda x: merge_dictionary(x[0], x[1]))
        self.writer.write_df(root_df, self.parser.name, self.parser.doc_type, self.parser.types)
