import os
from .lambdas import extract_metadata, extract_link, flatten_files_to_lists, get_props, get_props_empty_values


class Translator(object):
    """
    The main entry point into the index export process for the mutation indices
    """
    def __init__(self, sc, hdfs_path, writer):
        self.sc = sc
        self.writer = writer
        self.hdfs_path = hdfs_path

    def translate_table(self, table_name, get_zero_frame=None, props=None):
        df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(flatten_files_to_lists)
        df = df.map(extract_metadata)

        if get_zero_frame:
            if df.isEmpty():
                df = self.sc.parallelize([('__BLANK_ID__', '__BLANK_VALUE__')])  # to create the frame for empty node
            return df.mapValues(lambda x: [])
        if props is not None:
            if df.isEmpty():
                return df.mapValues(get_props_empty_values(props))
            names = {p.src: p.name for p in props}
            values = {p.src:  {m.original: m.final for m in p.value_mappings} for p in props}
            return df.mapValues(get_props(names, values))
        return df

    def translate_edge(self, table_name):
        df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(flatten_files_to_lists)
        return df.map(extract_link)
