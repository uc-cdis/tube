import json
import os.path

import yaml
from collections import namedtuple
from jsonschema import RefResolver
from pyspark.sql import SQLContext
from tube.etl.outputs.es.writer import Writer
from tube.utils.dd import init_dictionary
from tube.etl.indexers.aggregation.new_translator import Translator as AggregationTranslator
from tube.etl.indexers.injection.new_translator import Translator as InjectionTranslator

ResolverPair = namedtuple("ResolverPair", ["resolver", "source"])

def load_from_local_file_to_dataframe(spark_session, file_path):
    return spark_session.read.parquet(file_path)

def get_spark_session(spark_context):
    sql_context = SQLContext(spark_context)
    return sql_context.sparkSession

MAPPING_FILE = "etlMapping.yaml"
TEST_DATA_HOME = "./tests/dataframe_tests/test_data"

def initialize_mappings(schema_name, mapping_name):
    mappings = {}
    list_mappings = yaml.load(open(os.path.join(TEST_DATA_HOME, schema_name, MAPPING_FILE)), Loader=yaml.SafeLoader)
    for mapping in list_mappings["mappings"]:
        mappings[mapping.get("doc_type")] = mapping
    return mappings.get(mapping_name)


def mock_dictionary_url(schema_name):
    all_schema = json.load(open(os.path.join(TEST_DATA_HOME, schema_name, "schema.json")))
    schemas = {}
    resolvers = {}
    for key, schema in all_schema.items():
        schemas[key] = schema
        resolver = RefResolver("{}#".format(key), schema)
        resolvers[key] = ResolverPair(resolver, schema)
    return schemas, resolvers


def get_input_output_dataframes(spark_session, schema_name, input_parquet_file, output_parquet_file):
    input_df = (
        load_from_local_file_to_dataframe(
            spark_session,
            os.path.join(TEST_DATA_HOME, schema_name, "dataframe", input_parquet_file)
        )
        if input_parquet_file else None
    )
    expected_df = load_from_local_file_to_dataframe(
        spark_session,
        os.path.join(TEST_DATA_HOME, schema_name, "dataframe", output_parquet_file)
    )
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
    assert diff == [],  f"Differences: {diff}"

    assert_schema(expected_df, checking_df, diff)
    assert_data(expected_df, checking_df, diff, key_column)
    assert diff == [],  f"Differences: {diff}"
