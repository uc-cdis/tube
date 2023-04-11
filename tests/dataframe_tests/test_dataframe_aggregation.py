import pytest
from tests.dataframe_tests.util import (
    get_spark_session,
    assert_dataframe_equality,
    get_input_output_dataframes,
)
from tube.utils.general import get_node_id_name

@pytest.mark.parametrize("translator", [("ibdgc", "participant", "aggregation", [])], indirect=True)
def test_get_direct_children_with_parent(translator):
    input_df, expected_df = get_input_output_dataframes(
        get_spark_session(translator.sc),
        "ibdgc",
        "participant__0_Translator.translate_parent",
        "participant__0_Translator.get_direct_children"
    )
    result_df = translator.get_direct_children(input_df)
    assert_dataframe_equality(expected_df, result_df, get_node_id_name("participant"))

@pytest.mark.parametrize("translator", [("ibdgc", "participant", "aggregation", [])], indirect=True)
def test_ensure_project_id_exist_with_project_id_in_input_df(translator):
    """
    This function is to test function ensure_project_id_exist with etlMapping has project_id
    :param spark_context
    :return: It assert that the dataframe out of ensure_project_id_exist has project_id field
    """
    input_df, expected_df = get_input_output_dataframes(
        get_spark_session(translator.sc),
        "ibdgc",
        "participant__0_Translator.get_direct_children",
        "participant__0_Translator.ensure_project_id_exist"
    )
    result_df = translator.ensure_project_id_exist(input_df)
    assert_dataframe_equality(expected_df, result_df, get_node_id_name("participant"))

@pytest.mark.parametrize("translator", [("ibdgc", "project", "aggregation", [])], indirect=True)
def test_ensure_project_id_exist_without_project_id_in_input_df(translator):
    """
    This function is to test function ensure_project_id_exist
    :param spark_context
    :return: It assert that the dataframe out of ensure_project_id_exist has project_id field
    """
    input_df, expected_df = get_input_output_dataframes(
        get_spark_session(translator.sc),
        "ibdgc",
        "project__0_Translator.get_direct_children",
        "project__0_Translator.ensure_project_id_exist"
    )
    result_df = translator.ensure_project_id_exist(input_df)
    assert_dataframe_equality(expected_df, result_df, get_node_id_name("project"))
