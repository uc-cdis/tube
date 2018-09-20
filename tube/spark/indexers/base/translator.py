import os
from .lambdas import extract_metadata, extract_link, flatten_files_to_lists, get_fields, get_fields_empty_values


class Translator(object):
    """
    The main entry point into the index export process for the mutation indices
    """
    def __init__(self, sc, hdfs_path, writer):
        self.sc = sc
        self.writer = writer
        self.hdfs_path = hdfs_path

    def translate_table(self, table_name, get_zero_frame=None, fields=None):
        df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(flatten_files_to_lists)
        df = df.map(extract_metadata)

        if get_zero_frame:
            if df.isEmpty():
                df = self.sc.parallelize([('__BLANK_ID__', '__BLANK_VALUE__')])  # to create the frame for empty node
            return df.mapValues(lambda x: [])
        if fields is not None:
            if df.isEmpty():
                return df.mapValues(get_fields_empty_values(fields))
            return df.mapValues(get_fields(fields))
        return df

    def translate_edge(self, table_name):
        df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(flatten_files_to_lists)
        return df.map(extract_link)
