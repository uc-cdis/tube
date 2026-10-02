import yaml
from tube.etl.indexers.aggregation.new_translator import (
    Translator as AggregatorTranslator,
)
from tube.etl.indexers.injection.new_translator import Translator as InjectionTranslator
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

    try:
        # First transform pass. Keep each result on executor-local disk rather than
        # serializing to HDFS/Parquet and immediately reading it back later.
        for translator in list(translators.values()):
            df = translator.translate()
            if df is None:
                continue

            translator.set_current_dataframe(df)
            translator.current_step = 1

            if len(translator.parser.joining_nodes) > 0:
                need_to_join[translator.parser.doc_type] = translator
                translator_to_translators[translator.parser.doc_type] = [
                    j.joining_index for j in translator.parser.joining_nodes
                ]

        # Cross-index joining pass. Replace the translator's current DataFrame with
        # the joined version, again persisting DISK_ONLY rather than HDFS.
        for v in list(need_to_join.values()):
            if v.current_step <= 0:
                continue

            df = v.translate_joining_props(translators)
            v.set_current_dataframe(df)
            v.current_step += 1

        # Final transformations and OpenSearch writes now consume current_df via
        # load_from_hadoop_to_dateframe(), which transparently prefers the in-process
        # intermediate over the legacy HDFS path.
        for t in list(translators.values()):
            print(t.__dict__)
            if t.current_step <= 0:
                continue

            df = t.translate_final()
            t.write(df)

    finally:
        # Explicitly clean up all persisted intermediate DataFrames, including older
        # versions retained to avoid lineage recomputation during joining.
        for translator in list(translators.values()):
            translator.clear_current_dataframes()


def get_index_names(config):
    stream = open(config.MAPPING_FILE)
    mappings = yaml.load(stream, Loader=yaml.SafeLoader)
    return [m["name"] for m in mappings["mappings"]]
