from tube.etl.indexers.base.lambdas import (
    merge_and_fill_empty_props,
    merge_dictionary,
    swap_key_value,
)
from tube.etl.indexers.base.translator import Translator as BaseTranslator
from tube.etl.indexers.aggregation.lambdas import (
    intermediate_frame,
    merge_aggregate_with_reducer,
    seq_aggregate_with_reducer,
    get_frame_zero,
    get_normal_frame,
    get_single_frame_zero_by_func,
)
from tube.etl.indexers.base.prop import PropFactory
from tube.utils.general import (
    get_node_id_name,
    get_node_id_name_without_prefix,
    PROJECT_ID,
    PROJECT_CODE,
    PROGRAM_NAME,
)
from pyspark.sql.functions import sort_array, struct, collect_list, col
import pyspark.sql.functions as f
from .parser import Parser
from ..base.lambdas import (
    swap_property_as_key,
    make_key_from_property,
    f_collect_list_udf,
    f_collect_set_udf,
)


def prop_to_aggregated_fn(col_name, fn):
    if fn == "count" or fn == "sum":
        return f.sum(col_name).alias(col_name)


class Translator(BaseTranslator):
    def __init__(self, sc, hdfs_path, writer, mapping, model, dictionary):
        super(Translator, self).__init__(sc, hdfs_path, writer)
        self.parser = Parser(mapping, model, dictionary)

    def aggregate_intermediate_data_frame(self, node_name, child, child_df, edge_df):
        """
        Perform aggregation in the intermediate steps (attached to a AggregationNode - child node)
        :param child_df: rdd of the child table which will be aggregated
        :param edge_df: rdd of the edge connected from the parent to child node.
        :return:
        """
        expr = [
            self.reducer_to_agg_func_expr(self.parser.reducer_by_prop.get(n), n, False)
            for n in child_df.schema.names
            if n in self.parser.reducer_by_prop
        ]
        return (
            edge_df.join(child_df, on=get_node_id_name(child.name), how="left_outer")
            .groupBy(get_node_id_name(node_name))
            .agg(*expr)
        )

    def aggregate_with_count_on_edge_tbl(self, node_name, df, edge_df, child):
        """
        Do the aggregation which only based on the edge table (count, sum, ...)
        :param node_name: name of current node
        :param df: dataframe of current node created by the union of all edges to that node
        :param edge_df: current edge to count
        :param child: child node
        :return: return the aggregated dataframe
        """
        count_reducer = None
        for reducer in child.reducers:
            if reducer.prop.src is None and reducer.fn == "count":
                count_reducer = reducer
                break

        node_id = get_node_id_name(node_name)
        if count_reducer is None:
            # if there is no reducer, group by parent key and get out empty value
            count_df = edge_df.select(node_id).dropDuplicates([node_id])
        else:
            # if there is reducer, group by parent key and get out the number of children
            # only non-leaf nodes goes through this step
            count_df = (
                edge_df.groupBy(node_id)
                .count()
                .select(node_id, col("count").alias(child.name))
            )
            count_reducer.done = True
        # combine value lists new counted dataframe to existing one
        return (
            count_df
            if df is None
            else df.join(count_df, on=get_node_id_name(node_name))
        )

    def reducer_to_agg_func_expr(self, func_name, value, is_merging):
        if func_name == "count":
            if is_merging:
                return f.sum(col(value)).alias(value)
            return f.count(col(value)).alias(value)
        if func_name == "sum":
            return f.sum(col(value)).alias(value)
        if func_name == "set":
            if is_merging:
                return f_collect_set_udf(col(value)).alias(value)
            return f.collect_set(col(value)).alias(value)
        if func_name == "list":
            if is_merging:
                return f_collect_list_udf(col(value)).alias(value)
            return f.collect_list(col(value)).alias(value)
        if func_name == "min":
            return f.min(col(value)).alias(value)
        if func_name == "max":
            return f.min(col(value)).alias(value)

    def aggregate_with_child_tbl(self, df, parent_name, edge_df, child):
        child_df = self.translate_table_to_dataframe(
            child,
            props=[
                rd.prop
                for rd in child.reducers
                if not rd.done and rd.prop.src is not None
            ],
        )

        temp_df = edge_df.join(
            child_df, on=get_node_id_name(child.name), how="left_outer"
        )

        expr = [
            self.reducer_to_agg_func_expr(rd.fn, rd.prop.src, False)
            for rd in child.reducers
            if not rd.done and rd.prop.src is not None
        ]
        temp_df = temp_df.groupBy(get_node_id_name(parent_name)).agg(*expr)
        return df.join(temp_df, on=get_node_id_name(parent_name), how="left_outer")

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
            key_df = self.translate_table_to_dataframe(n, get_zero_frame=True)
            for child in n.children:
                if child.no_children_to_map == 0:
                    # Read all associations from edge table that link between parent and child one
                    edge_df = key_df.join(
                        self.translate_edge_to_dataframe(
                            child.edge_up_tbl, child.name, n.name
                        ),
                        on=get_node_id_name(n.name),
                        how="left_outer",
                    )
                    df = self.aggregate_with_count_on_edge_tbl(
                        n.name, df, edge_df, child
                    )
                    no_of_remaining_reducers = len(
                        [r for r in child.reducers if not r.done]
                    )
                    if no_of_remaining_reducers > 0:
                        df = self.aggregate_with_child_tbl(df, n.name, edge_df, child)

                    # aggregate values for child node (it is a sum for non-leaf node that has been in the hash,
                    # and a count leaf node)
                    df = (
                        df
                        if child.__key__() not in aggregated_dfs
                        else df.join(
                            self.aggregate_intermediate_data_frame(
                                n.name, child, aggregated_dfs[child.__key__()], edge_df
                            ),
                            on=get_node_id_name(n.name),
                        )
                    )
                    n.no_children_to_map -= 1
                    edge_df.unpersist()
                else:
                    df = key_df
            aggregated_dfs[n.__key__()] = df
            key_df.unpersist()
        return aggregated_dfs[self.parser.root.name]

    def get_direct_children(self, root_df):
        """
        Get data of all directed nodes and attach to root node
        :param root_df:
        :return:
        """
        root_id = get_node_id_name(self.parser.root.name)
        for n in self.parser.flatten_props:
            # if n is a child of root node, we don't need to swap order of the pair ids
            edge_df = self.translate_edge_to_dataframe(
                n.edge, n.name, self.parser.root.name
            )
            props = n.props
            if n.sorted_by is not None:
                sorting_prop = PropFactory.adding_prop(
                    self.parser.doc_type, n.sorted_by, n.sorted_by, []
                )
                props.append(sorting_prop)
            child_df = self.translate_table_to_dataframe(n, props=props)
            n_id = get_node_id_name(n.name)
            child_by_root = (
                edge_df.join(child_df, on=n_id, how="inner")
                .groupBy(root_id)
                .agg(
                    sort_array(
                        collect_list(struct(*child_df.columns)),
                        asc=False if n.desc_order else True,
                    )
                    .getItem(0)
                    .alias("sorted_col")
                )
            )
            child_by_root = child_by_root.select(root_id, "sorted_col.*")
            root_df = root_df.join(
                child_by_root, on=get_node_id_name(self.parser.root.name)
            )
            child_df.unpersist()
            child_by_root.unpersist()
        return root_df

    def get_joining_props(self, translator, joining_index):
        """
        Get joining props added by an additional join between indices/documents
        :param joining_index: Joining index created from parser
        :return:
        """
        props_with_fn = []
        props_without_fn = []
        for r in joining_index.getting_fields:
            src_prop = translator.parser.get_prop_by_name(r.prop.name)
            # field which is identity of a node is named as _{node}_id now
            # before in etl-mapping for joining_props, we use {node}_id
            # for backward compatibility, we check first with the value in mapping file.
            # if there is not any Prop object like that, we check with new format _{node}_id
            if src_prop is None and r.prop.src == get_node_id_name_without_prefix(
                translator.parser.doc_type
            ):
                src_prop = translator.parser.get_prop_by_name(
                    get_node_id_name(translator.parser.doc_type)
                )
            dst_prop = self.parser.get_prop_by_name(r.prop.name)
            if r.fn is None:
                props_without_fn.append({"src": src_prop, "dst": dst_prop})
            else:
                props_with_fn.append({"src": src_prop, "dst": dst_prop})
        return props_with_fn, props_without_fn

    def join_and_aggregate(self, df, joining_df, dual_props, joining_node):
        frame_zero = get_frame_zero(joining_node.getting_fields)
        joining_df = (
            self.get_props_from_df(joining_df, dual_props)
            .mapValues(get_normal_frame(joining_node.getting_fields))
            .aggregateByKey(
                frame_zero, seq_aggregate_with_reducer, merge_aggregate_with_reducer
            )
            .mapValues(lambda x: {x1: x2 for (x0, x1, x2) in x})
        )
        df = df.leftOuterJoin(joining_df).mapValues(
            lambda x: merge_and_fill_empty_props(x, [p.get("dst") for p in dual_props])
        )
        joining_df.unpersist()
        return df

    def join_no_aggregate(self, df, joining_df, dual_props):
        joining_df = self.get_props_from_df(joining_df, dual_props)
        df = df.leftOuterJoin(joining_df).mapValues(
            lambda x: merge_and_fill_empty_props(x, [p.get("dst") for p in dual_props])
        )
        joining_df.unpersist()
        return df

    def join_to_an_index(self, df, translator, joining_node):
        """
        Perform the join between indices. It will:
         - load rdd to be join from HDFS
         - Joining with df
        :param df: rdd of translator that does the join
        :param translator: translator has rdd to be join this translator
        :param joining_node: joining_node define in yaml file.
        :return:
        """
        joining_df = translator.load_from_hadoop_to_dateframe()

        # For joining two indices, we need to swap the property field and key of one of the index.
        # based on join_on value in the etlMapping, we know what field is used as joining field.
        # We swap the index that have name of key field different than the name of joining field
        # joining_df_key_id = translator.parser.get_key_prop().id
        # id_field_in_joining_df = translator.parser.get_prop_by_name(
        #     joining_node.joining_field
        # ).id
        # # field which is identity of a node is named as _{node}_id now
        # # before in etl-mapping for joining_props, we use {node}_id
        # # for backward compatibility, we check first with the value in mapping file.
        # # if there is not any Prop object like that, we check with new format _{node}_id
        # id_field_in_df = self.parser.get_prop_by_name(joining_node.joining_field)
        # if id_field_in_df is None:
        #     id_field_in_df = self.parser.get_prop_by_name(
        #         get_node_id_name(self.parser.doc_type)
        #     )
        # if id_field_in_df is None:
        #     raise Exception(
        #         "{} field does not exist in index {}".format(
        #             joining_node.joining_field, self.parser.doc_type
        #         )
        #     )
        # id_field_in_df_id = id_field_in_df.id
        # df_key_id = self.parser.get_key_prop().id
        #
        # swap_df = False
        # if joining_df_key_id != id_field_in_joining_df:
        #     joining_df = swap_property_as_key(
        #         joining_df, id_field_in_joining_df, joining_df_key_id
        #     )
        # if df_key_id != id_field_in_df_id:
        #     df = swap_property_as_key(df, id_field_in_df_id, df_key_id)
        #     swap_df = True
        #
        # # Join can be done with or without an aggregation function like max, min, sum, ...
        # # these two type of join requires different map-reduce steos
        props_with_fn, props_without_fn = self.get_joining_props(
            translator, joining_node
        )
        if len(props_with_fn) > 0:
            df = self.join_and_aggregate(df, joining_df, props_with_fn, joining_node)
        if len(props_without_fn) > 0:
            df = self.join_no_aggregate(df, joining_df, props_without_fn)
        return df

    def ensure_project_id_exist(self, df):
        project_id_prop = self.parser.get_prop_by_name(PROJECT_ID)
        if project_id_prop is None:
            project_id_prop = PropFactory.adding_prop(
                self.parser.doc_type, PROJECT_ID, None, [], prop_type=(str,)
            )
            project_code_id = self.parser.get_prop_by_name(PROJECT_CODE).id
            program_name_id = self.parser.get_prop_by_name(PROGRAM_NAME).id
            df = df.select(
                "*",
                ("{}-{}".format(col(program_name_id), col(project_code_id))).alias(
                    project_id_prop.id
                ),
            )
        return df

    def translate(self):
        # root_tbl = get_node_table_name(self.parser.model, self.parser.root)
        root_df = self.translate_table_to_dataframe(
            self.parser.root, props=self.parser.props
        )
        root_df = self.translate_parent(root_df)
        root_df = self.get_direct_children(root_df)
        root_df = self.ensure_project_id_exist(root_df)
        if len(self.parser.aggregated_nodes) == 0:
            return root_df
        return root_df.join(
            self.aggregate_nested_properties(),
            on=get_node_id_name(self.parser.root.name),
        )

    def translate_joining_props(self, translators):
        """
        Perform the join between the index/document created by this translator with
        the indices/documents created by translators in the paramter
        :param translators: translators containing indices that need to be joined
        :return:
        """
        df = self.load_froload_from_hadoop_to_dateframem_hadoop()
        for j in self.parser.joining_nodes:
            df = self.join_to_an_index(df, translators[j.joining_index], j)
        return df

    def translate_parent(self, root_df):
        if len(self.parser.parent_nodes) == 0:
            return root_df
        #        root_tbl = get_node_table_name(self.parser.model, self.parser.root)
        root_id = get_node_id_name(self.parser.root.name)
        for f in self.parser.parent_nodes:
            df = self.translate_table_to_dataframe(self.parser.root, props=[])
            src = self.parser.root
            n = f.head
            while n is not None:
                edge_tbl = n.edge_up_tbl
                df = df.join(
                    self.translate_edge_to_dataframe(edge_tbl, src.name, n.name),
                    on=get_node_id_name(src.name),
                    how="inner",
                )
                cur_props = n.props
                n_df = self.translate_table_to_dataframe(n, props=cur_props)
                df = n_df.join(df, on=get_node_id_name(n.name), how="inner")
                src = n
                n = n.child
            root_df = root_df.join(df, on=root_id, how="left_outer")
        return root_df
