import yaml
from .aggregation.new_translator import Translator as AggregatorTranslator
from .injection.new_translator import Translator as InjectionTranslator
from .nested.translator import Translator as NestedTranslator
from .base.translator import Translator as BaseTranslator
from tube.utils.dd import init_dictionary
from tube.etl.outputs.es.writer import Writer


def create_translators(sc, config):
    dictionary, model = init_dictionary(config.DICTIONARY_URL)
    mappings = yaml.load(open(config.MAPPING_FILE), Loader=yaml.SafeLoader)
    writer = Writer(sc, config)

    translators = {}
    for m in mappings["mappings"]:
        if m["type"] == "aggregator":
            translator = AggregatorTranslator(
                sc, config.HDFS_DIR, writer, m, model, dictionary
            )
        elif m["type"] == "collector":
            translator = InjectionTranslator(
                sc, config.HDFS_DIR, writer, m, model, dictionary
            )
        else:
            translator = BaseTranslator(sc, config.HDFS_DIR, writer)
        translators[translator.parser.doc_type] = translator
    for translator in list(translators.values()):
        translator.update_types()
    return translators


def run_transform(translators):
    need_to_join = {}
    translator_to_translators = {}

    for translator in list(translators.values()):
        df = translator.translate()
        if df is None:
            continue
        translator.save_to_hadoop(df)
        translator.current_step = 1
        if len(translator.parser.joining_nodes) > 0:
            need_to_join[translator.parser.doc_type] = translator
            translator_to_translators[translator.parser.doc_type] = [
                j.joining_index for j in translator.parser.joining_nodes
            ]

    for v in list(need_to_join.values()):
        df = v.translate_joining_props(translators)
        v.save_to_hadoop(df)
        v.current_step += 1

    for t in list(translators.values()):
        df = t.translate_final()
        t.write(df)


def get_index_names(config):
    stream = open(config.MAPPING_FILE)
    mappings = yaml.load(stream, Loader=yaml.SafeLoader)
    return [m["name"] for m in mappings["mappings"]]
