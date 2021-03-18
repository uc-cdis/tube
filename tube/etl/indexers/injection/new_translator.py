from copy import copy

from tube.etl.indexers.base.prop import PropFactory
from tube.etl.indexers.base.translator import Translator as BaseTranslator
from tube.etl.indexers.injection.parser import Parser
from tube.etl.indexers.injection.nodes.collecting_node import LeafNode
from tube.utils.general import PROJECT_ID, PROJECT_CODE, PROGRAM_NAME, get_node_id_name
from tube.utils.general import FILE_ID
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
                child, props=self.parser.props, get_zero_frame=True, key_name=FILE_ID
            )
            if len(child_df.head(1)) == 0:
                return
            child_df = child_df.withColumn("source_node", fn.lit(child.name))
            edge_df = edge_df.withColumnRenamed(get_node_id_name(child.name), FILE_ID)
            child_df = child_df.join(edge_df, on=FILE_ID)
            rm_props = [
                c
                for c in child_df.schema.names
                if c not in [PROJECT_ID, FILE_ID]
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
            collected_collecting_dfs[child.name] = self.join_two_dataframe(
                collected_collecting_dfs[child.name], child_df
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
                tmp_df = self.join_two_dataframe(df, child_df)
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
                            edge_df = self.join_two_dataframe(
                                df,
                                self.translate_edge_to_dataframe(
                                    edge_tbl, child.name, collector.name
                                ),
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
                if p.name != PROJECT_ID:
                    props.append(self.clone_prop_with_iterator_fn(p))
        return props

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

        expr = [
            self.reducer_to_agg_func_expr(p.fn, p.name, is_merging=False)
            for p in aggregating_props
        ]
        tmp_df = df.groupBy(FILE_ID).agg(*expr)

        rm_props = [p.name for p in aggregating_props if p.name != FILE_ID]
        df = df.drop(*rm_props)
        return self.join_two_dataframe(df, tmp_df)

    def write(self, df):
        self.writer.write_dataframe(
            df, self.parser.name, self.parser.doc_type, self.parser.types
        )
