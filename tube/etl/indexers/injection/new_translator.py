from copy import copy
from pyspark.sql import functions as fn
from pyspark.sql.types import StructField, StructType

from tube.etl.indexers.base.prop import PropFactory
from tube.etl.indexers.base.translator import Translator as BaseTranslator
from tube.etl.indexers.injection.parser import Parser
from tube.etl.indexers.injection.nodes.collecting_node import LeafNode
from tube.utils.general import PROJECT_ID, PROJECT_CODE, PROGRAM_NAME, get_node_id_name
from tube.utils.spark import get_hadoop_type_ignore_fn
from tube.settings import logger
from pyspark.sql.types import (
    StringType,
)


class Translator(BaseTranslator):
    def __init__(self, sc, hdfs_path, writer, mapping, model, dictionary):
        super(Translator, self).__init__(
            sc, hdfs_path, writer, Parser(mapping, model, dictionary)
        )
        self.parser.update_props_with_edges_having_data()
        root_props = []
        for root in self.parser.roots:
            root_props.extend(root.props)
        self.root_props = list(set(root_props))
        self.parser.additional_props.append(
            PropFactory.add_additional_prop(self.parser.doc_type, "source_node", (str,))
        )

    def collect_leaf(self, child, edge_df, collected_leaf_dfs):
        if isinstance(child, LeafNode):
            key_name = self.parser.get_key_prop().name
            key_prop = PropFactory.get_prop_by_name(self.parser.doc_type, key_name)
            props = []
            props.extend(child.props)
            props.append(key_prop)
            child_df = self.translate_table_to_dataframe(
                child,
                props=props,
                get_zero_frame=True,
                key_name=key_name,
            )
            if len(child_df.head(1)) == 0:
                return
            child_df = child_df.withColumn("source_node", fn.lit(child.name))
            edge_df = edge_df.withColumnRenamed(get_node_id_name(child.name), key_name)
            child_df = self.join_two_dataframe(edge_df, child_df, how="left_outer")
            #  child_df.join(edge_df, on=self.parser.get_key_prop().name)
            rm_props = [
                c
                for c in child_df.schema.names
                if c not in [PROJECT_ID, self.parser.get_key_prop().name, "source_node"]
                and not PropFactory.has_prop_in_doc_name(self.parser.doc_type, c)
            ]
            if len(child_df.head(1)) == 0:
                return
            child_df = child_df.drop(*rm_props)

            select_expr = child_df.columns
            prop_list = PropFactory.get_prop_by_doc_name(self.parser.doc_type)
            for p in prop_list.values():
                if p.name not in child_df.columns:
                    select_expr.append(
                        fn.lit(None).cast(get_hadoop_type_ignore_fn(p)).alias(p.name)
                    )
            if "final" in collected_leaf_dfs:
                logger.info(
                    f"collected_leaf_dfs types: {collected_leaf_dfs['final'].dtypes}"
                )
            child_df = child_df.select(*select_expr)
            collected_leaf_dfs["final"] = (
                child_df
                if "final" not in collected_leaf_dfs
                else collected_leaf_dfs["final"].unionByName(child_df).distinct()
            )
            if child.name in collected_leaf_dfs:
                collected_leaf_dfs[child.name].unpersist()
            child.done = True

    def collect_collecting_child(self, child, edge_df, collected_collecting_dfs):
        if edge_df is None or len(edge_df.head(1)) == 0:
            child.no_parent_to_map -= 1
            return
        child.no_parent_to_map -= 1
        if len(child.props) > 0:
            child_df = self.translate_table_to_dataframe(child, props=child.props)
            child_df = self.join_two_dataframe(edge_df, child_df)
        else:
            child_df = edge_df

        additional_props = []
        self.add_some_additional_props(additional_props)
        additional_props.append(get_node_id_name(child.name))
        rm_props = [
            p
            for p in child_df.schema.names
            if p not in PropFactory.get_prop_by_doc_name(self.parser.doc_type)
            and p not in additional_props
        ]
        if child.name not in collected_collecting_dfs:
            collected_collecting_dfs[child.name] = child_df.drop(*rm_props)
        else:
            collected_collecting_dfs[child.name] = self.join_two_dataframe(
                collected_collecting_dfs[child.name], child_df, how="full_outer"
            ).drop(*rm_props)
        self.return_dataframe(
            collected_collecting_dfs[child.name],
            f"{Translator.collect_collecting_child.__qualname__}__collected_collecting_dfs__{child.name}"
        )

    def merge_project(self, child, edge_df, collected_collecting_dfs):
        if edge_df is None or len(edge_df.head(1)) == 0:
            child.no_parent_to_map -= 1
            return
        child.no_parent_to_map -= 1
        child_df = self.translate_table_to_dataframe(child, props=child.props)
        child_df = self.join_two_dataframe(child_df, edge_df)

        child_df = child_df.withColumn(
            PROJECT_ID, fn.concat_ws("-", fn.col(PROGRAM_NAME), fn.col(PROJECT_CODE))
        )
        collected_collecting_dfs[child.name] = self.return_dataframe(
            child_df,
            f"{Translator.merge_project.__qualname__}__collected_collecting_dfs__{child.name}"
        )

    def join_program_to_project(self):
        collected_collecting_dfs = {}
        for root in self.parser.roots:
            df = self.translate_table_to_dataframe(root, props=root.props)
            if df.rdd.isEmpty():
                return collected_collecting_dfs
            for child in root.children:
                edge_tbl = child.parents[root.name]
                child_df = self.translate_edge_to_dataframe(
                    edge_tbl, child.name, root.name
                )
                tmp_df = self.join_two_dataframe(df, child_df)
                self.merge_project(child, tmp_df, collected_collecting_dfs)
        return collected_collecting_dfs

    def merge_collectors(self, collected_collecting_dfs):
        done_once = True
        while done_once:
            done_once = False
            for collector in self.parser.collectors:
                if not collector.done and collector.no_parent_to_map == 0:
                    df = collected_collecting_dfs.get(collector.name, None)
                    for child in collector.children:
                        edge_df = None
                        if df is not None:
                            edge_tbl = child.parents[collector.name]
                            edge_df = self.join_two_dataframe(
                                df,
                                self.translate_edge_to_dataframe(
                                    edge_tbl, child.name, collector.name
                                ),
                            )
                        self.collect_collecting_child(
                            child, edge_df, collected_collecting_dfs
                        )
                        if edge_df is not None:
                            edge_df.unpersist()
                    collector.done = True
                    done_once = True

    def get_leaves(self, collected_collecting_dfs, collected_leaf_dfs):
        for leaf in self.parser.leaves:
            df = collected_collecting_dfs.get(
                leaf.name, self.get_empty_dataframe_with_name(leaf)
            )
            self.collect_leaf(leaf, df, collected_leaf_dfs)

    def add_some_additional_props(self, keep_props):
        super(Translator, self).add_some_additional_props(keep_props)
        keep_props.append(PROJECT_ID)
        keep_props.append("source_node")

    def create_index_schema(self):
        data_types = {}
        for prop in self.parser.props:
            if prop.name not in data_types:
                data_types[prop.name] = get_hadoop_type_ignore_fn(prop)
        fields = []
        for k, v in data_types.items():
            fields.append(StructField(k, v, True))
        fields.append(
            StructField(get_node_id_name(self.parser.doc_type), StringType(), True)
        )
        return StructType(fields=fields)

    def create_empty_dataframe(self):
        schema = self.create_index_schema()
        return self.sql_context.createDataFrame(self.sc.emptyRDD(), schema)

    def translate(self):
        collected_collecting_dfs = self.join_program_to_project()
        self.merge_collectors(collected_collecting_dfs)
        collected_leaf_dfs = {}
        self.get_leaves(collected_collecting_dfs, collected_leaf_dfs)
        for k, df in list(collected_collecting_dfs.items()):
            if k != "final" and df is not None:
                df.unpersist()

        if "final" in collected_leaf_dfs:
            final_df = collected_leaf_dfs["final"]
        else:
            final_df = self.create_empty_dataframe()
        return self.return_dataframe(
            final_df,
            f"{Translator.translate.__qualname__}__collected_leaf_dfs"
        )

    def clone_prop_with_iterator_fn(self, p):
        prop = copy(self.parser.get_prop_by_name(p.name))
        if p.fn is not None:
            prop.fn = p.fn
        else:
            prop.fn = "set"
        return prop

    def get_aggregating_props(self):
        props = []
        for p in self.root_props:
            props.append(self.clone_prop_with_iterator_fn(p))

        for c in self.parser.collectors:
            for p in c.props:
                if p.name != PROJECT_ID:
                    props.append(self.clone_prop_with_iterator_fn(p))
        return props

    def flatten_nested_list(self, df, aggregating_props):
        expr = []
        for p in aggregating_props:
            if p.name in df.columns:
                if len(p.type) > 1 and p.type[0] is list and p.fn in ["list", "set"]:
                    expr.append(
                        self.reducer_to_agg_func_expr(
                            p.fn, p.name, is_merging=True, is_flattening=True
                        )
                    )
                else:
                    expr.append(
                        self.reducer_to_agg_func_expr(p.fn, p.name, is_merging=False)
                    )
        if len(expr) == 0:
            return df
        tmp_df = df.groupBy(self.parser.get_key_prop().name).agg(*expr)

        rm_props = [
            p.name
            for p in aggregating_props
            if p.name != self.parser.get_key_prop().name
        ]
        df = df.drop(*rm_props)
        final_df = self.join_two_dataframe(df, tmp_df)
        return final_df

    def translate_final(self):
        """
        Because one file can belong to multiple root nodes (case, subject).
        In the final step of file document, we must construct the list of root instance's id
        :return:
        """
        df = self.load_from_hadoop_to_dateframe()
        aggregating_props = self.get_aggregating_props()
        if len(aggregating_props) == 0:
            return df

        final_df = self.flatten_nested_list(df, aggregating_props)
        return self.return_dataframe(
            final_df,
            f"{Translator.translate_final.__qualname__}__translate_final"
        )
