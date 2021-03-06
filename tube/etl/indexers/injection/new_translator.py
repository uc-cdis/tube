from copy import copy

from tube.etl.indexers.base.lambdas import merge_dictionary
from tube.etl.indexers.base.prop import PropFactory
from tube.etl.indexers.base.translator import Translator as BaseTranslator
from tube.etl.indexers.injection.parser import Parser
from tube.etl.indexers.injection.lambdas import (
    get_props_to_tuple,
    seq_aggregate_with_prop,
    merge_aggregate_with_prop,
    remove_props_from_tuple,
    get_frame_zero,
)
from tube.etl.indexers.injection.nodes.collecting_node import LeafNode
from tube.utils.general import PROJECT_ID, PROJECT_CODE, PROGRAM_NAME, get_node_id_name
from pyspark.sql import functions as fn


class Translator(BaseTranslator):
    def __init__(self, sc, hdfs_path, writer, mapping, model, dictionary):
        super(Translator, self).__init__(sc, hdfs_path, writer)
        self.parser = Parser(mapping, model, dictionary)
        root_props = []
        for root in self.parser.roots:
            root_props.extend(root.props)
        self.root_props = list(set(root_props))
        self.parser.additional_props.append(
            PropFactory.add_additional_prop(self.parser.doc_type, "source_node", (str,))
        )

    def collect_leaf(self, child, edge_df, collected_leaf_dfs):
        if isinstance(child, LeafNode):
            child_df = self.translate_table_to_dataframe(
                child, props=self.parser.props, get_zero_frame=True
            )
            if len(child_df.head(1)) == 0:
                return
            child_df = child_df.withColumn("source_node", fn.lit(child.name))
            child_df = child_df.join(edge_df, on=get_node_id_name(child.name))
            rm_props = [
                c
                for c in child_df.schema.names
                if c != PROJECT_ID
                and not PropFactory.has_prop_in_doc_name(self.parser.doc_type, c)
            ]
            child_df = child_df.drop(*rm_props)
            collected_leaf_dfs["final"] = (
                child_df
                if "final" not in collected_leaf_dfs
                else collected_leaf_dfs["final"].union(child_df).distinct()
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
        else:
            child_df = edge_df
        if child.name not in collected_collecting_dfs:
            collected_collecting_dfs[child.name] = child_df
        else:
            join_on_props = [
                p
                for p in collected_collecting_dfs[child.name].schema.names
                if p in child_df.schema.names
            ]
            collected_collecting_dfs[child.name] = collected_collecting_dfs[
                child.name
            ].join(child_df, on=join_on_props)

    def merge_project(self, child, edge_df, collected_collecting_dfs):
        if edge_df is None or len(edge_df.head(1)) == 0:
            child.no_parent_to_map -= 1
            return
        child.no_parent_to_map -= 1
        child_node_id = get_node_id_name(child.name)
        child_df = self.translate_table_to_dataframe(child, props=child.props)
        child_df = child_df.join(edge_df, on=child_node_id)

        child_df = child_df.withColumn(
            PROJECT_ID, fn.concat_ws("-", fn.col(PROGRAM_NAME), fn.col(PROJECT_CODE))
        )
        collected_collecting_dfs[child.name] = child_df

    def join_program_to_project(self):
        collected_leaf_dfs = {}
        collected_collecting_dfs = {}
        for root in self.parser.roots:
            df = self.translate_table_to_dataframe(root, props=root.props)
            for child in root.children:
                edge_tbl = child.parents[root.name]
                child_df = self.translate_edge_to_dataframe(
                    edge_tbl, child.name, root.name
                )
                join_on_props = [
                    p for p in df.schema.names if p in child_df.schema.names
                ]
                tmp_df = df.join(
                    self.translate_edge_to_dataframe(edge_tbl, child.name, root.name),
                    on=join_on_props,
                )
                self.merge_project(child, tmp_df, collected_collecting_dfs)
        return collected_collecting_dfs, collected_leaf_dfs

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
                            edge_df = df.join(
                                self.translate_edge_to_dataframe(
                                    edge_tbl, child.name, collector.name
                                ),
                                on=get_node_id_name(collector.name),
                            )
                        self.collect_collecting_child(
                            child, edge_df, collected_collecting_dfs
                        )
                    collector.done = True
                    done_once = True

    def get_leaves(self, collected_collecting_dfs, collected_leaf_dfs):
        for leaf in self.parser.leaves:
            df = collected_collecting_dfs.get(
                leaf.name, self.get_empty_dateframe_with_name(leaf.name)
            )
            self.collect_leaf(leaf, df, collected_leaf_dfs)

    def translate(self):
        collected_collecting_dfs, collected_leaf_dfs = self.join_program_to_project()
        self.merge_collectors(collected_collecting_dfs)
        self.get_leaves(collected_collecting_dfs, collected_leaf_dfs)
        for (k, df) in list(collected_collecting_dfs.items()):
            if k != "final":
                df.unpersist()

        if "final" in collected_leaf_dfs:
            return collected_leaf_dfs["final"]
        else:
            return self.get_empty_dateframe_with_name(None)

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
                if p.name != "project_id":
                    props.append(self.clone_prop_with_iterator_fn(p))
        return props

    def translate_final(self):
        """
        Because one file can belong to multiple root nodes (case, subject).
        In the final step of file document, we must construct the list of root instance's id
        :return:
        """
        df = self.load_from_hadoop()
        aggregating_props = self.get_aggregating_props()
        if len(aggregating_props) == 0:
            return df
        return df
        # frame_zero = get_frame_zero(aggregating_props)
        #
        # prop_df = (
        #     df.mapValues(lambda x: get_props_to_tuple(x, aggregating_props))
        #     .aggregateByKey(
        #         frame_zero, seq_aggregate_with_prop, merge_aggregate_with_prop
        #     )
        #     .mapValues(lambda x: {x1: x2 for (x0, x1, x2) in x})
        # )
        #
        # df = (
        #     df.mapValues(lambda x: remove_props_from_tuple(x, aggregating_props))
        #     .distinct()
        #     .mapValues(lambda x: {x0: x1 for (x0, x1) in x})
        # )
        #
        # return df.join(prop_df).mapValues(lambda x: merge_dictionary(x[0], x[1]))
