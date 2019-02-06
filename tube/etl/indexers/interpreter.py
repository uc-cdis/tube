import yaml
from .aggregation.translator import Translator as AggregatorTranslator
from .injection.translator import Translator as CollectorTranslator
from .base.translator import Translator as BaseTranslator
from tube.utils.dd import init_dictionary
from tube.etl.outputs.es.writer import Writer


def create_translators(sc, config):
    dictionary, model = init_dictionary(config.DICTIONARY_URL)
    mappings = yaml.load(open(config.MAPPING_FILE))
    writer = Writer(sc, config)

    translators = {}
    for m in mappings['mappings']:
        if m['type'] == 'aggregator':
            translator = AggregatorTranslator(sc, config.HDFS_DIR, writer, m, model, dictionary)
        elif m['type'] == 'collector':
            translator = CollectorTranslator(sc, config.HDFS_DIR, writer, m, model)
        else:
            translator = BaseTranslator(sc, config.HDFS_DIR, writer)
        translators[translator.parser.doc_type] = translator
    return translators


def run_transform(translators):
    for translator in translators.values():
        translator.translate()


def get_index_names(config):
    stream = open(config.MAPPING_FILE)
    mappings = yaml.load(stream)
    return [m['name'] for m in mappings['mappings']]
