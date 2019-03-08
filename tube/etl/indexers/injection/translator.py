from tube.etl.indexers.base.lambdas import merge_and_fill_empty_props
from tube.etl.indexers.base.translator import Translator as BaseTranslator
from .nodes.collecting_node import LeafNode
from .parser import Parser


class Translator(BaseTranslator):
    def __init__(self, sc, hdfs_path, writer, mapping, model, dictionary):
        super(Translator, self).__init__(sc, hdfs_path, writer)
        self.parser = Parser(mapping, model, dictionary)
        root_props = []
        for root in self.parser.roots:
            root_props.extend(root.props)
        self.root_props = root_props

    def collect_child(self, child, edge_df, collected_collecting_dfs, collected_leaf_dfs, root_props=None):
        if edge_df is None or edge_df.isEmpty():
            child.no_parent_to_map -= 1
            return
        root_props = self.root_props if root_props is None else root_props
        if type(child) is LeafNode:
            # child_df = collected_dfs[child.name] if child.name in collected_dfs else \
            child_df = self.translate_table(child.tbl_name, props=self.parser.props)
            child_df = child_df.join(edge_df).mapValues(
                lambda x: merge_and_fill_empty_props(x, root_props))
            child.done = True
            collected_leaf_dfs['final'] = child_df if 'final' not in collected_leaf_dfs \
                else collected_leaf_dfs['final'].union(child_df).reduceByKey(lambda (x, y): x)
            if child.name in collected_leaf_dfs:
                collected_leaf_dfs[child.name].unpersist()
        else:
            child.no_parent_to_map -= 1
            if child.name not in collected_collecting_dfs:
                collected_collecting_dfs[child.name] = edge_df
            else:
                collected_collecting_dfs[child.name].union(edge_df)

    def merge_roots_to_children(self):
        collected_leaf_dfs = {}
        collected_collecting_dfs = {}
        for root in self.parser.roots:
            df = self.translate_table(root.tbl_name, props=root.props)
            root_name = root.name
            props = root.props
            for child in root.children:
                edge_tbl, _ = child.parents[root_name]
                edge_df = df.join(self.translate_edge(edge_tbl))\
                    .map(lambda x: (x[1][1], ({'{}_id'.format(root_name): x[0]},) + (x[1][0],)))\
                    .mapValues(lambda x: merge_and_fill_empty_props(x, props))
                self.collect_child(child, edge_df, collected_collecting_dfs, collected_leaf_dfs)
        return collected_collecting_dfs, collected_leaf_dfs

    def merge_collectors(self, collected_collecting_dfs, collected_leaf_dfs):
        done_once = True
        while done_once:
            done_once = False
            for collector in self.parser.collectors:
                if not collector.done and collector.no_parent_to_map == 0:
                    df = collected_collecting_dfs.get(collector.name, None)
                    for child in collector.children:
                        edge_df = None
                        if df is not None:
                            edge_tbl, _ = child.parents[collector.name]
                            edge_df = df.join(self.translate_edge(edge_tbl)) \
                                .map(lambda x: (x[1][1], x[1][0]))
                        self.collect_child(child, edge_df, collected_collecting_dfs, collected_leaf_dfs)
                    collector.done = True
                    done_once = True

    def get_leaves(self, collected_collecting_dfs, collected_leaf_dfs):
        for leaf in self.parser.leaves:
            df = collected_collecting_dfs.get(leaf.name, None)
            if df is None:
                continue
            # edge_df = df.join(self.translate_edge(leaf.edge_up_tbl))\
            #     .map(lambda x: (x[1][1], x[1][0]))
            self.collect_child(leaf, df, collected_collecting_dfs, collected_leaf_dfs)

    def translate(self):
        collected_collecting_dfs, collected_leaf_dfs = self.merge_roots_to_children()
        self.merge_collectors(collected_collecting_dfs, collected_leaf_dfs)
        self.get_leaves(collected_collecting_dfs, collected_leaf_dfs)
        for (k, df) in collected_collecting_dfs.items():
            if k != 'final':
                df.unpersist()
        if 'final' in collected_leaf_dfs:
            return collected_leaf_dfs['final']
        else:
            return self.sc.parallelize([])

    def translate_joining_props(self, translators):
        pass
