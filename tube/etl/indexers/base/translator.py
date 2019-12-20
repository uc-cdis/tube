import os
from .lambdas import extract_metadata, extract_link, extract_link_reverse, \
    flatten_files_to_lists, get_props, get_props_empty_values, get_number
from tube.utils.spark import save_rds
from .prop import PropFactory


class Translator(object):
    """
    The main entry point into the index export process for the mutation indices
    """
    def __init__(self, sc, hdfs_path, writer):
        self.sc = sc
        self.writer = writer
        self.hdfs_path = hdfs_path
        self.parser = None
        self.current_step = 0

    def update_types(self):
        self.parser.update_prop_types()
        self.parser.get_es_types()

    def translate_table(self, table_name, get_zero_frame=None, props=None):
        try:
            df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(flatten_files_to_lists)
            df = df.map(extract_metadata)

            if get_zero_frame:
                if df.isEmpty():
                    df = self.sc.parallelize([('__BLANK_ID__', '__BLANK_VALUE__')])  # to create the frame for empty node
                return df.mapValues(lambda x: [])
            if props is not None:
                return self.get_props_from_data_row(df, props)
            return df
        except Exception as ex:
            print("HAPPEN WITH NODE: {}".format(table_name))
            print(ex)

    def translate_edge(self, table_name, reversed=True):
        """
        Return the edge table that has two columns.
        :param table_name:
        :return: [(child_node_id, parent_node_id)] if not reversed other wise [(parent_node_id, child_node_id)]
        """
        df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(flatten_files_to_lists)
        if reversed:
            return df.map(extract_link_reverse)
        return df.map(extract_link)

    def write(self, df):
        df = self.restore_prop_name(df, PropFactory.list_props)
        self.writer.write_df(df, self.parser.name, self.parser.doc_type, self.parser.types)
        self.writer.create_guppy_array_config(self.parser.name, self.parser.types)

    def get_props_from_data_row(self, df, props, to_tuple=False):
        if df.isEmpty():
            return df.mapValues(get_props_empty_values(props))
        names = {p.src: p.id for p in props}
        values = {p.src: {m.original: m.final for m in p.value_mappings} for p in props}
        return df.mapValues(get_props(names, values))

    def get_props_from_df(self, df, props):
        if df.isEmpty():
            return df.mapValues(get_props_empty_values([p.get('dst') for p in props]))
        prop_ids = [(p.get('src').id, p.get('dst').id) for p in props]
        return df.mapValues(lambda x: {dst: x.get(src) for (src, dst) in prop_ids})

    def restore_prop_name(self, df, props):
        return df.mapValues(lambda x: {props[k].name if isinstance(get_number(k), int) else k: v for (k, v) in list(x.items())})

    def get_path_from_step(self, step):
        return os.path.join(self.hdfs_path, 'output', '{}_{}'.format(self.parser.doc_type, str(step)))

    def save_to_hadoop(self, df):
        save_rds(df, self.get_path_from_step(self.current_step), self.sc)
        df.unpersist()

    def load_from_hadoop(self):
        return self.sc.pickleFile(self.get_path_from_step(self.current_step - 1))

    def translate_joining_props(self, translators):
        pass

    def translate_final(self):
        return self.load_from_hadoop()
