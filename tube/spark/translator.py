import os
import ast
import json
from tube.utils import make_sure_hdfs_path_exist


def extract_metadata(str_value):
    strs = ast.literal_eval(str_value.replace('""', "'"))
    props = json.loads(strs[3].replace("'", '"'))
    props['id'] = strs[4]
    return props


def extract_link(str_value):
    strs = ast.literal_eval(str_value)
    props = {}
    props['src_id'] = strs[4]
    props['dst_id'] = strs[5]
    return props


def flatten_files_to_lists(pair):
    f, text = pair
    return [line for line in text.splitlines()]


class Gen3Translator(object):
    """
    The main entry point into the index export process for the mutation indices
    """
    def __init__(self, sc, config):
        self.config = config
        self.logger = config.logger
        self.sc = sc
        self.hdfs_path = make_sure_hdfs_path_exist(config.HDFS_DIR, sc)

    def translate_table(self, table_name):
        df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(flatten_files_to_lists)
        df = df.map(extract_metadata)
        df.saveAsTextFile(os.path.join(self.hdfs_path, 'export', table_name))

    def translate_edge(self, table_name):
        df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(flatten_files_to_lists)
        df = df.map(extract_link)
        df.saveAsTextFile(os.path.join(self.hdfs_path, 'export', table_name))

    def run_etl(self):
        with open(self.config.LIST_TABLES_FILES) as f:
            lines = f.readlines()
            for line in lines:
                line = line.rstrip('\n')
                print(line)
                if line.startswith('node_'):
                    self.translate_table(line)
                if line.startswith('edge_'):
                    self.translate_edge(line)
