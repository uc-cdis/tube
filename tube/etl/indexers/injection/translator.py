from copy import copy
from tube.etl.indexers.base.lambdas import merge_and_fill_empty_props, merge_dictionary
from tube.etl.indexers.base.translator import Translator as BaseTranslator
from .nodes.collecting_node import LeafNode
from .parser import Parser
from .lambdas import get_props_to_tuple, remove_props_from_tuple, get_frame_zero, \
    seq_aggregate_with_prop, merge_aggregate_with_prop, construct_project_id
from tube.etl.indexers.base.prop import PropFactory


class Translator(BaseTranslator):
    def __init__(self, sc, hdfs_path, writer, mapping, model, dictionary):
        super(Translator, self).__init__(sc, hdfs_path, writer)
        self.parser = Parser(mapping, model, dictionary)
        root_props = []
        for root in self.parser.roots:
            root_props.extend(root.props)
        self.root_props = list(set(root_props))

    def collect_leaf(self, child, edge_df, collected_leaf_dfs, root_props=None):
        root_props = self.root_props if root_props is None else root_props
        if isinstance(child, LeafNode):
            child_df = self.translate_table(child.tbl_name, props=self.parser.props)
            if child_df.isEmpty():
                return
            child_df = child_df.join(edge_df).mapValues(
                lambda x: merge_and_fill_empty_props(x, root_props, to_tuple=True))
            collected_leaf_dfs['final'] = child_df if 'final' not in collected_leaf_dfs \
                else collected_leaf_dfs['final'].union(child_df).distinct()
            if child.name in collected_leaf_dfs:
                collected_leaf_dfs[child.name].unpersist()
            child.done = True

    def collect_collecting_child(self, child, edge_df, collected_collecting_dfs):
        if edge_df is None or edge_df.isEmpty():
            child.no_parent_to_map -= 1
            return
        child.no_parent_to_map -= 1
        if child.name not in collected_collecting_dfs:
            collected_collecting_dfs[child.name] = edge_df
        else:
            collected_collecting_dfs[child.name] = collected_collecting_dfs[child.name].union(edge_df).distinct()

    def merge_auth_root(self, root):
        df = self.translate_table(root.tbl_name, props=root.props)
        child = root.root_child
        props = copy(root.props)
        while child is not None:
            edge_tbl = child.edge_to_parent
            child_props = child.props
            df = df.join(self.translate_edge(edge_tbl)) \
                .map(lambda x: (x[1][1], x[1][0]))
            tbl_name = child.tbl_name
            df = df.join(self.translate_table(tbl_name, props=child_props)) \
                .mapValues(lambda x: merge_and_fill_empty_props(x, child_props))
            props.extend(child_props)
            child = child.root_child
        project_id_prop = self.parser.get_prop_by_name('project_id')
        if project_id_prop is None:
            project_id_prop = PropFactory.adding_prop(self.parser.doc_type, 'project_id', None, [])
        root_id = project_id_prop.id
        return df.mapValues(lambda x: construct_project_id(x, props, root_id))

    def merge_roots_to_children(self):
        collected_leaf_dfs = {}
        collected_collecting_dfs = {}
        for root in self.parser.roots:
            if root.root_child is None:
                df = self.translate_table(root.tbl_name, props=root.props)
                root_id = self.parser.get_prop_by_name('{}_id'.format(root.name)).id
            else:
                df = self.merge_auth_root(root)
            props = root.props
            for child in root.children:
                edge_tbl = child.parents[root.name]
                tmp_df = df.join(self.translate_edge(edge_tbl))
                if root.root_child is None:
                    tmp_df = tmp_df.map(lambda x: (x[1][1], ({root_id: x[0]},) + (x[1][0],)))\
                        .mapValues(lambda x: merge_and_fill_empty_props(x, props, to_tuple=True))
                else:
                    tmp_df = tmp_df.map(lambda x: (x[1][1], x[1][0])) \
                        .mapValues(lambda x: tuple([(k, v) for (k, v) in list(x.items())]))
                self.collect_collecting_child(child, tmp_df, collected_collecting_dfs)
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
                            edge_df = df.join(self.translate_edge(edge_tbl)) \
                                .map(lambda x: (x[1][1], x[1][0]))
                        self.collect_collecting_child(child, edge_df, collected_collecting_dfs)
                    collector.done = True
                    done_once = True

    def get_leaves(self, collected_collecting_dfs, collected_leaf_dfs):
        for leaf in self.parser.leaves:
            df = collected_collecting_dfs.get(leaf.name, self.sc.parallelize([('__BLANK_ID__', '__BLANK_VALUE__')]))
            self.collect_leaf(leaf, df, collected_leaf_dfs)

    def translate(self):
        collected_collecting_dfs, collected_leaf_dfs = self.merge_roots_to_children()
        self.merge_collectors(collected_collecting_dfs)
        self.get_leaves(collected_collecting_dfs, collected_leaf_dfs)
        for (k, df) in list(collected_collecting_dfs.items()):
            if k != 'final':
                df.unpersist()

        if 'final' in collected_leaf_dfs:
            collected_leaf_dfs['final'] = collected_leaf_dfs['final'].mapValues(lambda x: {x_i[0]: x_i[1] for x_i in x})
            return collected_leaf_dfs['final']
        else:
            return self.sc.parallelize([])

    def get_aggregating_props(self):
        props = []
        for p in self.root_props:
            prop = copy(self.parser.get_prop_by_name(p.name))
            if p.fn is not None:
                prop.fn = p.fn
            else:
                prop.fn = 'set'
            props.append(prop)
        return props

    def translate_final(self):
        '''
        Because one file can belong to multiple root nodes (case, subject).
        In the final step of file document, we must construct the list of root instance's id
        :return:
        '''
        df = self.load_from_hadoop()
        aggregating_props = self.get_aggregating_props()
        if len(aggregating_props) == 0:
            return df

        frame_zero = get_frame_zero(aggregating_props)

        prop_df = df.mapValues(lambda x: get_props_to_tuple(x, aggregating_props))\
            .aggregateByKey(frame_zero, seq_aggregate_with_prop, merge_aggregate_with_prop)\
            .mapValues(lambda x: {x1: x2 for (x0, x1, x2) in x})

        df = df.mapValues(lambda x: remove_props_from_tuple(x, aggregating_props)).distinct()\
            .mapValues(lambda x: {x0: x1 for (x0, x1) in x})

        return df.join(prop_df).mapValues(lambda x: merge_dictionary(x[0], x[1]))
