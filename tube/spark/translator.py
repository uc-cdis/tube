import ast


def extract_metadata(str_value):
    strs = str_value.split(',')
    print(strs)
    s3 = ','.join(strs[3:len(strs)-1])
    props = ast.literal_eval(s3.replace('""', "'").replace('"', ''))
    props['id'] = strs[len(strs) - 1]
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
        self.hdfs_path = config.HDFS_DIR

    def run_etl(self, config=None):
        df = self.sc.wholeTextFiles(self.hdfs_path + '/node_case').flatMap(flatten_files_to_lists)
        print(df.first())
        res = extract_metadata(df.first())
        print(res)
        df = df.map(extract_metadata)
        df.saveAsTextFile(self.hdfs_path + '/export')
