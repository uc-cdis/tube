import yaml
from pyspark.sql import SQLContext
from tube.etl.outputs.es.writer import Writer
from tube.utils.dd import init_dictionary
from tube.etl.indexers.aggregation.new_translator import Translator as AggregationTranslator
from tube.etl.indexers.injection.new_translator import Translator as InjectionTranslator

def load_from_local_file_to_dataframe(spark_session, file_path):
    return spark_session.read.parquet(file_path)

def get_spark_session(spark_context):
    sql_context = SQLContext(spark_context)
    return sql_context.sparkSession

MAPPING_FILE = "./tests/dataframe_tests/test_data/etlMapping.yaml"
TEST_DATA_HOME = "./tests/dataframe_tests/test_data"
mappings = {}

def initialize_mappings():
    list_mappings = yaml.load(open(MAPPING_FILE), Loader=yaml.SafeLoader)
    for mapping in list_mappings["mappings"]:
        mappings[mapping.get("doc_type")] = mapping

def get_mapping(mapping_name):
    return mappings.get(mapping_name)

def get_translator(spark_context, config, mapping_name, indexer_type):
    dictionary, model = init_dictionary(config.DICTIONARY_URL)
    writer = Writer(spark_context, config)
    mapping = get_mapping(mapping_name)
    hdfs_path = f"{TEST_DATA_HOME}/graphs"
    if indexer_type.lower() == "aggregation":
        return AggregationTranslator(spark_context, hdfs_path, writer, mapping, model, dictionary)
    elif indexer_type.lower() == "injection":
        return InjectionTranslator(spark_context, hdfs_path, writer, mapping, model, dictionary)


def get_input_output_dataframes(spark_session, input_parquet_file, output_parquet_file):
    input_df = (
        load_from_local_file_to_dataframe(spark_session, f"{TEST_DATA_HOME}/dataframe/{input_parquet_file}")
        if input_parquet_file else None
    )
    expected_df = load_from_local_file_to_dataframe(spark_session, f"{TEST_DATA_HOME}/dataframe/{output_parquet_file}")
    return input_df, expected_df


def schema_to_dict_fields(schema):
    dict_fs = {}
    for f in schema:
        dict_fs[f.name] = f
    return dict_fs


def assert_schema(expected_df, checking_df, diff):
    expected_fields = schema_to_dict_fields(expected_df.schema)
    checking_fields = schema_to_dict_fields(checking_df.schema)
    for k, v in expected_fields.items():
        if k not in checking_fields:
            diff.append(f"Schema field expected vs real value: {v} is not in checking value")
        elif v.dataType != checking_fields.get(k).dataType:
            diff.append(f"Schema field expected vs real value: {v} != {checking_fields.get(k)}")
    for k, v in checking_fields.items():
        if k not in expected_fields:
            diff.append(f"Schema field expected vs real value: {v} is not in expected value")


def assert_null(expected_df, checking_df, diff):
    if expected_df is None or checking_df is None:
        if checking_df is None:
            diff.append(f"Expected dataframe vs real dataframe: {expected_df.collect()} != {checking_df}")
        if expected_df is None:
            diff.append(f"Expected dataframe vs real dataframe: {expected_df} != {checking_df.collect()}")



def assert_data(expected_df, checking_df, diff, key_column):
    expected_collection = expected_df.sort(key_column).collect()
    checking_collection = checking_df.sort(key_column).collect()
    zip_collection = zip(expected_collection, checking_collection)
    for expected, checking in zip_collection:
        if expected != checking:
            diff.append(f"Datarow expected vs real value: {expected} != {checking}")


def assert_dataframe_equality(expected_df, checking_df, key_column):
    diff = []
    if expected_df is None and checking_df is None:
        return

    assert_null(expected_df, checking_df, diff)
    print(f"Differences: {diff}")
    assert diff == []

    assert_schema(expected_df, checking_df, diff)
    assert_data(expected_df, checking_df, diff, key_column)
    print(f"Differences: {diff}")
    assert diff == []
