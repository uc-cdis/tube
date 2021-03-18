from tube.etl.indexers.base.translator import Translator as BaseTranslator
from tube.etl.indexers.base.prop import PropFactory, Prop
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
        expr = []
        for n in child_df.schema.names:
            if n in self.parser.reducer_by_prop:
                if self.parser.reducer_by_prop.get(n) in ["list", "set"]:
                    expr.append(
                        self.reducer_to_agg_func_expr(
                            self.parser.reducer_by_prop.get(n), n, is_merging=False
                        )
                    )
                else:
                    expr.append(
                        self.reducer_to_agg_func_expr(
                            self.parser.reducer_by_prop.get(n), n, is_merging=True
                        )
                    )
        tmp_df = (
            self.join_two_dataframe(edge_df, child_df, how="left_outer")
            .groupBy(get_node_id_name(node_name))
            .agg(*expr)
        )

        select_expr = [get_node_id_name(node_name)]
        for n in child_df.schema.names:
            if n in self.parser.reducer_by_prop and self.parser.reducer_by_prop.get(
                n
            ) in ["list", "set"]:
                select_expr.append(
                    self.reducer_to_agg_func_expr(
                        self.parser.reducer_by_prop.get(n), n, is_merging=True
                    )
                )
        tmp_df = tmp_df.select(*select_expr)
        return self.join_two_dataframe(edge_df, tmp_df)

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
        return count_df if df is None else self.join_two_dataframe(df, count_df)

    def aggregate_with_child_tbl(self, df, parent_name, edge_df, child):
        child_df = self.translate_table_to_dataframe(
            child,
            props=[
                rd.prop
                for rd in child.reducers
                if not rd.done and rd.prop.src is not None
            ],
        )

        temp_df = self.join_two_dataframe(edge_df, child_df, how="left_outer")
        expr = [
            self.reducer_to_agg_func_expr(rd.fn, rd.prop.name, is_merging=False)
            for rd in child.reducers
            if not rd.done and rd.prop.src is not None
        ]
        temp_df = temp_df.groupBy(get_node_id_name(parent_name)).agg(*expr)
        return self.join_two_dataframe(df, temp_df, how="left_outer")

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
                    edge_df = self.join_two_dataframe(
                        key_df,
                        self.translate_edge_to_dataframe(
                            child.edge_up_tbl, child.name, n.name
                        ),
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
                        else self.join_two_dataframe(
                            df,
                            self.aggregate_intermediate_data_frame(
                                n.name, child, aggregated_dfs[child.__key__()], edge_df
                            ),
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
            child_by_root = self.join_two_dataframe(edge_df, child_df)

            if n.sorted_by is not None:
                sorted_cols = []
                for c in child_df.schema.names:
                    if c == n.sorted_by:
                        sorted_cols.append(c)
                for c in child_df.schema.names:
                    if c != n.sorted_by:
                        sorted_cols.append(c)
                child_by_root = child_by_root.groupBy(root_id).agg(
                    sort_array(
                        collect_list(struct(*sorted_cols)),
                        asc=False if n.desc_order else True,
                    )
                    .getItem(0)
                    .alias("sorted_col")
                )
                child_by_root = child_by_root.select(root_id, "sorted_col.*")
            root_df = self.join_two_dataframe(root_df, child_by_root)
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
                src_prop = translator.parser.get_prop_by_name(r.prop.src)
                if src_prop is None:
                    src_prop = Prop(
                        PropFactory.get_additional_length(),
                        r.prop.src,
                        None,
                        [],
                        None,
                        None,
                        None,
                        (str,),
                        is_additional=True,
                    )
            dst_prop = self.parser.get_prop_by_name(r.prop.name)
            if r.fn is None:
                props_without_fn.append({"src": src_prop, "dst": dst_prop})
            else:
                props_with_fn.append({"src": src_prop, "dst": dst_prop})
        return props_with_fn, props_without_fn

    def join_and_aggregate(self, df, joining_df, dual_props, joining_node):
        src_col_names = [p.get("src").name for p in dual_props]
        src_col_names.append(joining_node.joining_field)
        joining_df = joining_df.select(src_col_names)

        expr = [
            self.reducer_to_agg_func_expr(
                p.fn, p.prop.src, alias=p.prop.name, is_merging=False
            )
            for p in joining_node.getting_fields
        ]
        tmp_df = joining_df.groupBy(joining_node.joining_field).agg(*expr)

        rm_props = [p for p in src_col_names if p != joining_node.joining_field]
        joining_df = joining_df.drop(*rm_props).join(
            tmp_df, on=joining_node.joining_field
        )

        df = df.join(joining_df, on=joining_node.joining_field)
        joining_df.unpersist()
        return df

    def join_no_aggregate(self, df, joining_df, dual_props, joining_node):
        expr = [col(p.get("src").name).alias(p.get("dst").name) for p in dual_props]
        joining_df = joining_df.select(*expr)
        df = df.join(joining_df, on=joining_node.joining_field)
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
        props_with_fn, props_without_fn = self.get_joining_props(
            translator, joining_node
        )
        if len(props_with_fn) > 0:
            df = self.join_and_aggregate(df, joining_df, props_with_fn, joining_node)
        if len(props_without_fn) > 0:
            df = self.join_no_aggregate(df, joining_df, props_without_fn, joining_node)
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
        agg_df = self.aggregate_nested_properties()
        root_id = get_node_id_name(self.parser.root.name)
        rm_props = [
            p for p in agg_df.schema.names if p in root_df.schema.names and p != root_id
        ]
        agg_df = agg_df.drop(*rm_props)
        if len(self.parser.aggregated_nodes) == 0:
            return root_df
        return self.join_two_dataframe(root_df, agg_df)

    def translate_joining_props(self, translators):
        """
        Perform the join between the index/document created by this translator with
        the indices/documents created by translators in the paramter
        :param translators: translators containing indices that need to be joined
        :return:
        """
        df = self.load_from_hadoop_to_dateframe()
        for j in self.parser.joining_nodes:
            df = self.join_to_an_index(df, translators[j.joining_index], j)
        return df

    def translate_parent(self, root_df):
        if len(self.parser.parent_nodes) == 0:
            return root_df
        #        root_tbl = get_node_table_name(self.parser.model, self.parser.root)
        for f in self.parser.parent_nodes:
            df = self.translate_table_to_dataframe(self.parser.root, props=[])
            src = self.parser.root
            n = f.head
            while n is not None:
                edge_tbl = n.edge_up_tbl
                df = self.join_two_dataframe(
                    df, self.translate_edge_to_dataframe(edge_tbl, src.name, n.name)
                )
                cur_props = n.props
                n_df = self.translate_table_to_dataframe(n, props=cur_props)
                df = self.join_two_dataframe(df, n_df)
                src = n
                n = n.child
            root_df = self.join_two_dataframe(root_df, df, how="left_outer")
        return root_df

    def translate_final(self):
        return self.load_from_hadoop_to_dateframe()

    def write(self, df):
        self.writer.write_dataframe(
            df, self.parser.name, self.parser.doc_type, self.parser.types
        )
