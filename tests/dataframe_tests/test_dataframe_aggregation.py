import tube.settings as config
from tests.dataframe_tests.util import (
    get_spark_session,
    initialize_mappings,
    get_translator,
    assert_dataframe_equality,
    get_input_output_dataframes
)
from tube.utils.general import get_node_id_name

DICTIONARY_URL = "https://s3.amazonaws.com/dictionary-artifacts/ibdgc-dictionary/1.6.10/schema.json"
initialize_mappings()

def test_get_direct_children_with_parent(spark_context):
    input_df, expected_df = get_input_output_dataframes(
        get_spark_session(spark_context),
        "participant__0_Translator.translate_parent",
        "participant__0_Translator.get_direct_children"
    )
    translator = get_translator(spark_context, config, "participant", "aggregation")
    result_df = translator.get_direct_children(input_df)
    assert_dataframe_equality(expected_df, result_df, get_node_id_name("participant"))

def test_ensure_project_id_exist_with_project_id_in_input_df(spark_context):
    """
    This function is to test function ensure_project_id_exist with etlMapping has project_id
    :param spark_context
    :return: It assert that the dataframe out of ensure_project_id_exist has project_id field
    """
    input_df, expected_df = get_input_output_dataframes(
        get_spark_session(spark_context),
        "participant__0_Translator.get_direct_children",
        "participant__0_Translator.ensure_project_id_exist"
    )
    translator = get_translator(spark_context, config, "participant", "aggregation")
    result_df = translator.ensure_project_id_exist(input_df)
    assert_dataframe_equality(expected_df, result_df, get_node_id_name("participant"))

def test_ensure_project_id_exist_without_project_id_in_input_df(spark_context):
    """
    This function is to test function ensure_project_id_exist
    :param spark_context
    :return: It assert that the dataframe out of ensure_project_id_exist has project_id field
    """
    input_df, expected_df = get_input_output_dataframes(
        get_spark_session(spark_context),
        "project__0_Translator.get_direct_children",
        "project__0_Translator.ensure_project_id_exist"
    )
    translator = get_translator(spark_context, config, "project", "aggregation")
    result_df = translator.ensure_project_id_exist(input_df)
    assert_dataframe_equality(expected_df, result_df, get_node_id_name("project"))

# def test_aggregate_nested_properties(spark_context):
#     _, expected_df = get_input_output_dataframes(
#         get_spark_session(spark_context),
#         None,
#         "participant__0_Translator.ensure_project_id_exist"
#     )
#     translator = get_translator(spark_context, config, "participant", "aggregation")
#     result_df = translator.aggregate_nested_properties()
#     assert_dataframe_equality(expected_df, result_df)
