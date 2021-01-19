from tube.etl.indexers.base.translator import Translator as BaseTranslator
from tube.etl.indexers.nested.parser import Parser
from tube.utils.general import get_node_id_name
from pyspark.sql.functions import struct, collect_list


class Translator(BaseTranslator):
    def __init__(self, sc, hdfs_path, writer, mapping, model, dictionary):
        super(Translator, self).__init__(sc, hdfs_path, writer)
        self.parser = Parser(mapping, model, dictionary)
        self.collected_node_dfs = {}

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
        node_df = self.translate_table_to_dataframe(node)
        node_name = node.name
        node_id_field = get_node_id_name(node_name)
        if node_df is None:
            return node_df
        for child in node.children:
            child_name = child.name
            if child_name in self.collected_node_dfs:
                child_df = self.collected_node_dfs[child_name]
                if node_id_field in child_df.columns:
                    node_df = self.collect_structural_df(
                        node_df, node_name, child_df, child_name
                    )
        for name, edge_up_tbl in node.parent_edge_up_tbl.items():
            edge_df = self.collect_edge(edge_up_tbl, node.name, name)
            if edge_df is not None:
                node_df = node_df.join(edge_df, on=node_id_field, how="left_outer")
        for name, parent in node.parent_nodes.items():
            parent.children_ready_to_join.append(node)
            if len(parent.children_ready_to_join) == len(parent.children):
                queue.append(parent)
        return node_df

    def collect_structural_df(self, node_df, node_name, child_df, child_name):
        id_field = get_node_id_name(node_name)
        node_df = node_df.join(
            child_df.groupBy(id_field).agg(
                collect_list(struct(*child_df.columns)).alias("{}".format(child_name))
            ),
            on=id_field,
        )
        return node_df

    def collect_edge(self, edge_tbl, src, dst):
        node_df = self.translate_edge_to_dataframe(edge_tbl, src, dst)
        return node_df

    def write(self, df):
        self.writer.write_dataframe(
            df, self.parser.name, self.parser.doc_type, self.parser.types
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
