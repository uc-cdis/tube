import yaml
from tube.utils import make_sure_hdfs_path_exist
from .aggregator.translator import Translator as AggregatorTranslator
from .collector.translator import Translator as CollectorTranslator
from .base.translator import Translator as BaseTranslator
from tube.utils import init_dictionary


class Interpreter(object):
    """
    The main entry point into the index export process for the mutation indices
    """
    def __init__(self, sc, writer, config):
        self.sc = sc
        self.writer = writer
        self.dictionary, self.model = init_dictionary(config.DICTIONARY_URL)
        if sc is not None:
            self.hdfs_path = make_sure_hdfs_path_exist(config.HDFS_DIR, sc)
        else:
            self.hdfs_path = config.HDFS_DIR
        self.translators = self.create_translators(config.MAPPING_FILE)

    def create_translators(self, mapping_path):
        stream = open(mapping_path)
        mappings = yaml.load(stream)
        translators = {}
        for m in mappings['mappings']:
            if m['type'] == 'aggregator':
                translator = AggregatorTranslator(self.sc, self.hdfs_path, self.writer, m, self.model, self.dictionary)
            elif m['type'] == 'collector':
                translator = CollectorTranslator(self.sc, self.hdfs_path, self.writer, m, self.model)
            else:
                translator = BaseTranslator(self.sc, self.hdfs_path, self.writer)
            translators[translator.parser.doc_type] = translator
        return translators

    def run_etl(self):
        for translator in self.translators.values():
            translator.translate()
