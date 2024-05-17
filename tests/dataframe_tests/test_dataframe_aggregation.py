import pytest
from tests.util import (
    get_spark_session,
    assert_dataframe_equality,
    assert_zero,
    get_dataframes_from_names,
    all_match
)
from tube.utils.general import get_node_id_name
from pyspark.sql.types import ArrayType, BooleanType
from pyspark.sql.functions import array_contains, udf

@pytest.mark.schema_ibdgc
@pytest.mark.parametrize("translator", [("ibdgc", "participant", "aggregation", [])], indirect=True)
def test_get_direct_children_with_parent(translator):
    """
    This function is to test function get_direct_children_with parent
    :param translator: define translator that is used in the test
    :return:
    """
    [input_df, expected_df] = get_dataframes_from_names(
        get_spark_session(translator.sc),
        "ibdgc",
        ["participant__0_Translator.translate_parent",
        "participant__0_Translator.get_direct_children"]
    )
    result_df = translator.get_direct_children(input_df)
    assert_dataframe_equality(expected_df, result_df, get_node_id_name("participant"))

@pytest.mark.schema_ibdgc
@pytest.mark.parametrize("translator", [("ibdgc", "participant", "aggregation", [])], indirect=True)
def test_ensure_project_id_exist_with_project_id_in_input_df(translator):
    """
    This function is to test function ensure_project_id_exist with etlMapping has project_id
    :param translator: define translator that is used in the test
    :return: It assert that the dataframe out of ensure_project_id_exist has project_id field
    """
    [input_df, expected_df] = get_dataframes_from_names(
        get_spark_session(translator.sc),
        "ibdgc",
        ["participant__0_Translator.get_direct_children",
        "participant__0_Translator.ensure_project_id_exist"]
    )
    result_df = translator.ensure_project_id_exist(input_df)
    assert_dataframe_equality(expected_df, result_df, get_node_id_name("participant"))

@pytest.mark.schema_ibdgc
@pytest.mark.parametrize("translator", [("ibdgc", "project", "aggregation", [])], indirect=True)
def test_ensure_project_id_exist_without_project_id_in_input_df(translator):
    """
    This function is to test function ensure_project_id_exist
    :param translator to define translator that is used in the test
    :return: It assert that the dataframe out of ensure_project_id_exist has project_id field
    """
    [input_df, expected_df] = get_dataframes_from_names(
        get_spark_session(translator.sc),
        "ibdgc",
        ["project__0_Translator.get_direct_children",
        "project__0_Translator.ensure_project_id_exist"]
    )
    result_df = translator.ensure_project_id_exist(input_df)
    assert_dataframe_equality(expected_df, result_df, get_node_id_name("project"))

@pytest.mark.schema_ibdgc
@pytest.mark.parametrize("translator", [("ibdgc", "participant", "aggregation", [])], indirect=True)
def test_translate_parent(translator):
    """
    This function is to test function translate_parent of aggregation translator
    :param translator to define translator that is used in the test
    :return: It assert that the translate parent working as expected
    """
    [input_df, expected_df] = get_dataframes_from_names(
        get_spark_session(translator.sc),
        "ibdgc",
        ["participant__0_Translator.translate_table_to_dataframe__participant",
        "participant__0_Translator.translate_parent"]
    )
    result_df = translator.translate_parent(input_df)
    assert_dataframe_equality(expected_df, result_df, get_node_id_name("participant"))

@pytest.mark.schema_midrc
@pytest.mark.parametrize("translator", [("midrc", "imaging_study", "aggregation", [])], indirect=True)
def test_translate_count_aggregation(translator):
    [expected_df] = get_dataframes_from_names(
        get_spark_session(translator.sc),
        "midrc",
        [
            "imaging_study__0_Translator.aggregate_nested_properties"
        ]
    )
    result_df = translator.aggregate_nested_properties()

    assert_dataframe_equality(expected_df, result_df, get_node_id_name("imaging_study"))
    diff = []
    assert_zero(result_df, diff, ["_dx_series_file_count", "_mr_series_file_count"])
    assert diff == [], f"Differences: {diff}"

@pytest.mark.schema_parent
@pytest.mark.parametrize("translator", [("parent", "participant", "aggregation", [])], indirect=True)
def test_flatten_nested_array_parent_props(translator):
    """
    Test to ensure the created dataframe will not contains any array being nested in another array
    - input dataframe is the data of root_node (participant)
    - we will test after calling translate_parent, it will produce the array field without nested array
    - based on the data that we have
        participant with id: 80cc940b-414f-4361-ac9f-24a94279e379 recruited by
        center with submitter_id: "4658f8c1-d50c-4651-99b6-4a934fe26783" has two projects
         with code:  jenkins (which has data_type: ["csv", "json"], and test (which has data_type: ["tsv", "json"])
    - expected data_type of participant 80cc940b-414f-4361-ac9f-24a94279e379 is ["csv", "tsv", "json"]

    :param translator:
    :return:
    """
    print("Start parent testing")
    [input_df, expected_df] = get_dataframes_from_names(
        get_spark_session(translator.sc),
        "parent",
        ["participant__0_Translator.translate_table_to_dataframe__participant",
        "participant__0_Translator.translate_parent"]
    )
    result_df = translator.translate_parent(input_df)
    print(result_df.show(truncate=False))
    field = result_df.schema["data_type"]
    assert isinstance(field.dataType, ArrayType)
    filter_df = result_df.filter(result_df._participant_id == "80cc940b-414f-4361-ac9f-24a94279e379").select("data_type")
    data_types = filter_df.first()["data_type"]
    print(data_types)
    assert all_match(data_types, ["csv", "tsv", "json"]) is True
