from tube.spark.indexers.base.lambdas import merge_and_fill_empty_props
from tube.spark.indexers.base.translator import Translator as BaseTranslator
from .nodes.collecting_node import LeafNode
from .parser import Parser


class Translator(BaseTranslator):
    def __init__(self, sc, hdfs_path, writer, mapping, model):
        super(Translator, self).__init__(sc, hdfs_path, writer)
        self.parser = Parser(mapping, model)

    def collect_child(self, child, edge_df, collected_dfs):
        if edge_df.isEmpty():
            return
        if type(child) is LeafNode:
            child_df = collected_dfs[child.name] if child.name in collected_dfs else\
                self.translate_table(child.tbl_name, props=self.parser.props)
            props = self.parser.final_fields
            child_df = child_df.leftOuterJoin(edge_df).mapValues(
                lambda x: merge_and_fill_empty_props(x, props))
            child.no_parent_to_map -= 1
            if child.no_parent_to_map == 0:
                child.done = True
                collected_dfs['final'] = child_df if 'final' not in collected_dfs \
                    else collected_dfs['final'].union(child_df)
                if child.name in collected_dfs:
                    del collected_dfs[child.name]
            else:
                collected_dfs[child.name] = child_df
        else:
            child_df = edge_df
            child.no_parent_to_map -= 1
            collected_dfs[child.name] = child_df

    def merge_roots_to_children(self):
        collected_dfs = {}
        for root in self.parser.roots:
            df = self.translate_table(root.tbl_name, props=root.props)
            root_name = root.name
            props = root.props + self.parser.props
            for child in root.children:
                edge_df = df.rightOuterJoin(self.translate_edge(child.edge_up_tbl))\
                    .map(lambda x: (x[1][1], ({'{}_id'.format(root_name): x[0]},) + (x[1][0],)))\
                    .mapValues(lambda x: merge_and_fill_empty_props(x, props))
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

    def translate(self):
        collected_dfs = self.merge_roots_to_children()
        self.merge_collectors(collected_dfs)
        self.get_leaves(collected_dfs)
        self.writer.write_df(collected_dfs['final'], self.parser.name, self.parser.doc_type, self.parser.types)
