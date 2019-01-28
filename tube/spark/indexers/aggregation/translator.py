from tube.spark.indexers.base.lambdas import merge_and_fill_empty_props, merge_dictionary, swap_key_value
from tube.spark.indexers.base.translator import Translator as BaseTranslator
from tube.utils import get_node_table_name
from tube.spark.indexers.aggregation.lambdas import intermediate_frame, merge_aggregate_with_reducer, \
    seq_aggregate_with_reducer, get_frame_zero, get_normal_frame, get_single_frame_zero_by_func
from ..base.lambdas import sort_by_field
from .parser import Parser
from memory_profiler import profile


class Translator(BaseTranslator):
    def __init__(self, sc, hdfs_path, writer, mapping, model, dictionary):
        super(Translator, self).__init__(sc, hdfs_path, writer)
        self.parser = Parser(mapping, model, dictionary)

    def aggregate_intermediate_data_frame(self, child_df, edge_df):
        frame_zero = tuple([get_single_frame_zero_by_func(i[0], i[1]) for i in child_df.first()[1]])
        temp_df = edge_df.leftOuterJoin(child_df).map(lambda x: (x[1][0], x[1][1])) \
            .mapValues(lambda x: x if x is not None else frame_zero)
        return temp_df.aggregateByKey(frame_zero,
                                      seq_aggregate_with_reducer,
                                      merge_aggregate_with_reducer)

    def aggregate_with_count_on_edge_tbl(self, df, edge_df, child):
        count_reducer = None
        for reducer in child.reducers:
            if reducer.prop.src is None and reducer.fn == 'count':
                count_reducer = reducer
                break

        if count_reducer is None:
            # if there is no reducer, group by parent key and get out empty value
            count_df = edge_df.groupByKey().mapValues(lambda x: ())
        else:
            # if there is no reducer, group by parent key and get out the number of children
            # only non-leaf nodes goes through this step
            count_df = edge_df.groupByKey().mapValues(lambda x: len([i for i in x if i is not None])) \
                .mapValues(intermediate_frame(count_reducer.prop.name))
            count_reducer.done = True
        # combine value lists new counted dataframe to existing one
        return count_df if df is None else df.leftOuterJoin(count_df).mapValues(lambda x: x[0] + x[1])

    def aggregate_with_child_tbl(self, df, swapped_df, child):
        props = []
        for rd in child.reducers:
            props.append(rd.prop.src)

        child_df = self.translate_table(child.tbl_name, props=[rd.prop for rd in child.reducers
                                                               if not rd.done and rd.prop.src is not None])\
            .mapValues(get_normal_frame(child.reducers))

        frame_zero = get_frame_zero(child.reducers)
        temp_df = swapped_df.leftOuterJoin(child_df).map(lambda x: (x[1][0], x[1][1]))\
            .mapValues(lambda x: x if x is not None else frame_zero)\
            .aggregateByKey(frame_zero, seq_aggregate_with_reducer, merge_aggregate_with_reducer)
        return df.leftOuterJoin(temp_df).mapValues(lambda x: x[0] + x[1])

    @profile
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
                    # Read all associations from edge table that link between parent and child one
                    edge_df = key_df.leftOuterJoin(self.translate_edge(child.edge_up_tbl)).mapValues(lambda x: x[1])
                    df = self.aggregate_with_count_on_edge_tbl(df, edge_df, child)
                    no_of_remaining_reducers = len([r for r in child.reducers if not r.done])
                    if no_of_remaining_reducers > 0:
                        df = self.aggregate_with_child_tbl(df, swap_key_value(edge_df), child)

                    # aggregate values for child node (it is a sum for non-leaf node that has been in the hash,
                    # and a count leaf node)
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
        """
        Get data of all directed nodes and attach to root node
        :param root_df:
        :return:
        """
        for n in self.parser.flatten_props:
            # if n is a child of root node, we don't need to swap order of the pair ids
            edge_df = self.translate_edge(n.edge, not n.props_from_child)
            child_df = self.translate_table(n.tbl_name, props=n.props)
            child_by_root = edge_df.join(child_df).map(lambda x: tuple([x[1][0], x[1][1]]))
            if n.sorted_by is not None:
                child_by_root = child_by_root.groupByKey()
                child_by_root = child_by_root.mapValues(lambda it: sort_by_field(it, n.sorted_by, n.desc_order)[0])
                child_by_root = child_by_root.mapValues(lambda x: {(k, v) for (k, v) in x.items()
                                                                   if not (k.startswith('_') and k.endswith('_'))})
            root_df = root_df.leftOuterJoin(child_by_root).mapValues(lambda x: merge_and_fill_empty_props(x, n.props))
        return root_df

    @profile
    def translate(self):
        root_tbl = get_node_table_name(self.parser.model, self.parser.root)
        root_df = self.translate_table(root_tbl, props=self.parser.props)
        print root_df.toDebugString()
        root_df = self.get_direct_children(root_df)
        print root_df.toDebugString()
        root_df = root_df.join(self.aggregate_nested_properties()).mapValues(lambda x: merge_dictionary(x[0], x[1]))
        print root_df.toDebugString()
        self.writer.write_df(root_df, self.parser.name, self.parser.doc_type, self.parser.types)
