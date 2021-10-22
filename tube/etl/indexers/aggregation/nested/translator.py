from tube.etl.indexers.base.translator import Translator as BaseTranslator
from tube.etl.indexers.aggregation.nested.parser import Parser
from tube.utils.general import get_node_id_name, replace_dot_with_dash
from pyspark.sql.functions import struct, collect_list


class Translator(BaseTranslator):
    def __init__(self, sc, hdfs_path, writer, mapping, model, dictionary):
        super(Translator, self).__init__(sc, hdfs_path, writer)
        self.parser = Parser(mapping, model, dictionary)
        self.collected_node_dfs = {}
        self.updated_types = False

    def update_types(self):
        if self.updated_types:
            return self.parser.types
        self.parser.update_prop_types()
        self.parser.get_es_types()
        self.updated_types = True
        return self.parser.types

    def collect_tree(self):
        print("Start transforming the tree")
        queue = []
        for l in self.parser.leaves:
            print(l)
            queue.append(l)
        i: int = 0
        while i < len(queue):
            df = self.collect_node(queue[i], queue)
            if df is not None:
                self.collected_node_dfs[queue[i].name] = df
            i += 1
        return self.collected_node_dfs[queue[len(queue) - 1].name]

    def collect_node(self, node, queue):
        node_df = self.translate_table_to_dataframe(node, node.props)
        node_name = node.name
        node_id_field = get_node_id_name(node_name)
        if node_df is None:
            return node_df
        for child in node.children:
            if child.name in self.collected_node_dfs:
                child_df = self.collected_node_dfs[child.name]
                if node_id_field in child_df.columns:
                    node_df = self.collect_structural_df(
                        node_df, node_name, child_df, child
                    )
        current_node_name = node_name
        for parent_label, edge_up_tbl in node.parent_edge_up_tbl:
            edge_df = self.collect_edge(edge_up_tbl, current_node_name, parent_label)
            if edge_df is not None:
                node_df = self.join_two_dataframe(edge_df, node_df)
            current_node_name = parent_label
        parent = node.parent_node
        if parent is not None:
            parent.children_ready_to_join.append(node)
            if len(parent.children_ready_to_join) == len(parent.children):
                queue.append(parent)
        return node_df

    def collect_structural_df(self, node_df, node_name, child_df, child):
        id_field = get_node_id_name(node_name)
        child_name = child.name
        cols = self.get_cols_from_node(child_name, child.props, child_df)
        node_df = node_df.join(
            child_df.groupBy(id_field).agg(
                collect_list(struct(*cols)).alias(child.display_name)
            ),
            on=id_field,
        )
        return node_df

    def collect_edge(self, edge_tbl, src, dst):
        node_df = self.translate_edge_to_dataframe(edge_tbl, src, dst)
        return node_df

    def write(self, df):
        self.writer.write_dataframe(
            df,
            self.parser.name,
            self.parser.doc_type,
            self.parser.types,
            self.parser.prop_types,
        )

    def translate(self):
        return self.collect_tree()

    def translate_final(self):
        """
        Because one file can belong to multiple root nodes (case, subject).
        In the final step of file document, we must construct the list of root instance's id
        :return:
        """
        pass
