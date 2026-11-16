import pytest
from tests.util import (
    get_spark_session,
    assert_dataframe_equality,
    assert_zero,
    get_dataframes_from_names,
    all_match
)
from tube.etl.indexers.aggregation.nodes.joining_node import JoiningNode
from tube.etl.indexers.base.prop import Prop
from tube.utils.general import get_node_id_name
from pyspark.sql.types import ArrayType, BooleanType, StringType, StructField, StructType
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

@pytest.mark.schema_midrc
@pytest.mark.parametrize("translator", [("midrc", "imaging_study", "aggregation", [])], indirect=True)
def test_join_and_aggregate_counts_documents_not_rows(translator):
    """
    Test that a 'count' joining prop counts documents of the joining index.

    join_to_an_index reads the joining index at its intermediate step, before that
    index runs its own translate_final and flattens down to one row per document.
    A file linked to several parents is still one row per link at that point, so
    counting rows counts the same file once per link.

    Here "file_1" is linked to three parents and "file_2" to one: the joining
    dataframe holds four rows, but study_1 owns two files. study_2 owns none, which
    must read as 0 rather than null, and the join must not duplicate study rows.

    One of the rows of "file_1" carries a different data_type, so the repeated rows
    are not all identical: deduplicating the projection is not enough to get the
    count right, it has to be counted over the key of the joining index.

    :param translator: define translator that is used in the test
    :return:
    """
    spark_session = get_spark_session(translator.sc)
    root_id = get_node_id_name("imaging_study")
    file_id = get_node_id_name("data_file")

    joining_df = spark_session.createDataFrame(
        [
            ("study_1", "file_1", "CT"),
            ("study_1", "file_1", "CT"),
            ("study_1", "file_1", "MR"),
            ("study_1", "file_2", "MR"),
        ],
        schema=StructType([
            StructField(root_id, StringType(), True),
            StructField(file_id, StringType(), True),
            StructField("data_type", StringType(), True),
        ])
    )
    input_df = spark_session.createDataFrame(
        [("study_1", "submitter_1"), ("study_2", "submitter_2")],
        schema=StructType([
            StructField(root_id, StringType(), True),
            StructField("submitter_id", StringType(), True),
        ])
    )

    # props of the index doing the join, as the parser builds them from joining_props
    count_prop = Prop(
        0, "data_file_count", file_id, [], None,
        src_index="data_file", fn="count", prop_type=(float,)
    )
    data_type_prop = Prop(
        1, "data_type", "data_type", [], None,
        src_index="data_file", fn="set", prop_type=(str,)
    )
    joining_node = JoiningNode(
        [count_prop, data_type_prop], {"index": "data_file", "join_on": root_id}
    )
    # props of the joined index, resolved by get_joining_props
    dual_props = [
        {"src": Prop(2, file_id, file_id, [], None, prop_type=(str,)), "dst": count_prop},
        {"src": Prop(3, "data_type", "data_type", [], None, prop_type=(str,)), "dst": data_type_prop},
    ]

    result_df = translator.join_and_aggregate(
        input_df, joining_df, dual_props, joining_node, file_id
    )
    print(result_df.show(truncate=False))

    rows = {r[root_id]: r for r in result_df.collect()}
    assert result_df.count() == 2, "the join must not duplicate the joining index rows"
    assert rows["study_1"]["data_file_count"] == 2
    assert all_match(rows["study_1"]["data_type"], ["CT", "MR"]) is True
    assert rows["study_2"]["data_file_count"] == 0
    assert rows["study_2"]["data_type"] is None

@pytest.mark.schema_midrc
@pytest.mark.parametrize("translator", [("midrc", "imaging_study", "aggregation", [])], indirect=True)
def test_join_and_aggregate_counts_on_a_src_that_is_not_the_key(translator):
    """
    Test a 'count' joining prop whose src is an ordinary field instead of the key of
    the joining index. It counts the documents holding a value for that field, the
    way counting rows did before, so neither the repeated rows nor the number of
    distinct values may leak into it.

    "file_1" is linked to two parents and carries a data_type, "file_2" carries the
    same data_type and "file_3" none: two documents are typed, out of three rows
    that have a data_type and one single distinct value of it.

    :param translator: define translator that is used in the test
    :return:
    """
    spark_session = get_spark_session(translator.sc)
    root_id = get_node_id_name("imaging_study")
    file_id = get_node_id_name("data_file")

    joining_df = spark_session.createDataFrame(
        [
            ("study_1", "file_1", "CT"),
            ("study_1", "file_1", "CT"),
            ("study_1", "file_2", "CT"),
            ("study_1", "file_3", None),
        ],
        schema=StructType([
            StructField(root_id, StringType(), True),
            StructField(file_id, StringType(), True),
            StructField("data_type", StringType(), True),
        ])
    )
    input_df = spark_session.createDataFrame(
        [("study_1", "submitter_1")],
        schema=StructType([
            StructField(root_id, StringType(), True),
            StructField("submitter_id", StringType(), True),
        ])
    )

    # the key of the joining index is not among the props, so join_and_aggregate has
    # to add it to the projection itself
    typed_count_prop = Prop(
        0, "typed_file_count", "data_type", [], None,
        src_index="data_file", fn="count", prop_type=(float,)
    )
    dual_props = [
        {"src": Prop(1, "data_type", "data_type", [], None, prop_type=(str,)),
         "dst": typed_count_prop},
    ]
    joining_node = JoiningNode(
        [typed_count_prop], {"index": "data_file", "join_on": root_id}
    )

    result_df = translator.join_and_aggregate(
        input_df, joining_df, dual_props, joining_node, file_id
    )
    print(result_df.show(truncate=False))

    assert result_df.count() == 1
    assert result_df.first()["typed_file_count"] == 2
