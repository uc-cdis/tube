import json
import os.path

import yaml
from collections import namedtuple
from jsonschema import RefResolver
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

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
    selected_schema_path = os.path.join(TEST_DATA_HOME, schema_name, "schema.json")
    print(selected_schema_path)
    all_schema = json.load(open(selected_schema_path))

    schemas = {}
    resolvers = {}
    for key, schema in all_schema.items():
        schemas[key] = schema
        resolver = RefResolver("{}#".format(key), schema)
        resolvers[key] = ResolverPair(resolver, schema)
    print(schema)
    return schemas, resolvers


def get_dataframes_from_names(spark_session, schema_name, parquet_files):
    dataframes = []
    for parquest_file in parquet_files:
        dataframes.append(load_from_local_file_to_dataframe(
            spark_session,
            os.path.join(TEST_DATA_HOME, schema_name, "dataframe", parquest_file)
        ))
    return dataframes


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
            checking_type = checking_fields.get(k).dataType
            # Dataframe loaded from an existing file will have some minor difference in schema
            # nullable vs not nullable for ArrayType field. This block is to resolve this minor difference
            if (
                isinstance(v.dataType, ArrayType) and isinstance(checking_type, ArrayType)
                and v.dataType.elementType == checking_type.elementType
            ):
                continue
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
