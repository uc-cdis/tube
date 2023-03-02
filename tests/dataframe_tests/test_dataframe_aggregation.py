import tube.settings as config
from util import (
    get_spark_session,
    initialize_mappings,
    get_translator,
    assert_dataframe_equality,
    get_input_output_dataframes
)

DICTIONARY_URL = "https://s3.amazonaws.com/dictionary-artifacts/ibdgc-dictionary/1.6.10/schema.json"
TEST_DATA_HOME = "./tests/dataframe_tests/test_data"
initialize_mappings()

def test_get_direct_children_without_parent(spark_context):
    input_df, expected_df = get_input_output_dataframes(
        get_spark_session(spark_context),
        f"{TEST_DATA_HOME}/participant__0_Translator.translate_table_to_dataframe__participant",
        f"{TEST_DATA_HOME}/participant__0_Translator.get_direct_children"
    )
    translator = get_translator(spark_context, config, "participant")
    result_df = translator.get_direct_children(input_df)
    assert_dataframe_equality(expected_df, result_df)

def test_ensure_project_id_exist(spark_context):
    input_df, expected_df = get_input_output_dataframes(
        get_spark_session(spark_context),
        f"{TEST_DATA_HOME}/participant__0_Translator.get_direct_children",
        f"{TEST_DATA_HOME}/participant__0_Translator.ensure_project_id_exist"
    )
    translator = get_translator(spark_context, config, "participant")
    result_df = translator.ensure_project_id_exist(input_df)
    assert_dataframe_equality(expected_df, result_df)

def test_aggregate_nested_properties(spark_context):
    _, expected_df = get_input_output_dataframes(
        get_spark_session(spark_context),
        None,
        f"{TEST_DATA_HOME}/participant__0_Translator.ensure_project_id_exist"
    )
    translator = get_translator(spark_context, config, "participant")
    result_df = translator.aggregate_nested_properties()
    assert_dataframe_equality(expected_df, result_df)
