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

def test_ensure_project_id_exist(spark_context):
    input_df, expected_df = get_input_output_dataframes(
        get_spark_session(spark_context),
        "participant__0_Translator.get_direct_children",
        "participant__0_Translator.ensure_project_id_exist"
    )
    translator = get_translator(spark_context, config, "participant", "aggregation")
    result_df = translator.ensure_project_id_exist(input_df)
    assert_dataframe_equality(expected_df, result_df, get_node_id_name("participant"))

# def test_aggregate_nested_properties(spark_context):
#     _, expected_df = get_input_output_dataframes(
#         get_spark_session(spark_context),
#         None,
#         "participant__0_Translator.ensure_project_id_exist"
#     )
#     translator = get_translator(spark_context, config, "participant", "aggregation")
#     result_df = translator.aggregate_nested_properties()
#     assert_dataframe_equality(expected_df, result_df)
