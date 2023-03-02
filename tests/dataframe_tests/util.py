import yaml
from pyspark.sql import SQLContext
from tube.etl.outputs.es.writer import Writer
from tube.utils.dd import init_dictionary
from tube.etl.indexers.aggregation.new_translator import Translator as AggregationTranslator

def load_from_local_file_to_dataframe(spark_session, file_path):
    return spark_session.read.parquet(file_path)

def get_spark_session(spark_context):
    sql_context = SQLContext(spark_context)
    yield sql_context.sparkSession

MAPPING_FILE = "./tests/dataframe_tests/test_data/etl-mapping.yaml"
TEST_DATA_HOME = "./tests/dataframe_tests/test_data"
mappings = {}

def initialize_mappings():
    list_mappings = yaml.load(MAPPING_FILE, Loader=yaml.SafeLoader)
    for mapping in list_mappings:
        mappings[mapping.get("doc_type")] = mapping

def get_mapping(mapping_name):
    return mappings.get(mapping_name)

def get_translator(spark_context, config, mapping_name):
    dictionary, model = init_dictionary(config.DICTIONARY_URL)
    writer = Writer(spark_context, config)
    mapping = get_mapping(mapping_name)
    return AggregationTranslator(spark_context, config.HDFS_DIR, writer, mapping, dictionary, model)


def get_input_output_dataframes(spark_session, input_parquet_file, output_parquet_file):
    input_df = (
        load_from_local_file_to_dataframe(input_parquet_file, spark_session)
        if input_parquet_file else None
    )
    expected_df = load_from_local_file_to_dataframe(output_parquet_file, spark_session)
    return input_df, expected_df


def assert_dataframe_equality(expected_df, checking_df):
    diff = []
    if expected_df.schema != checking_df.schema:
        diff.append(f"Schema expected vs real value: {expected_df.schema} != {checking_df.schema}")
    if expected_df.schema != checking_df.schema:
        diff.append(f"Dataframe expected vs real value: {expected_df.collect()} != {checking_df.collect()}")
    assert diff == []
