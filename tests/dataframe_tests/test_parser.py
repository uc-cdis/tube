import pytest
from tests.util import (
    get_spark_session,
    assert_dataframe_equality,
    get_dataframes_from_names,
)
from tube.utils.general import get_node_id_name


@pytest.mark.schema_midrc
@pytest.mark.parametrize("translator", [("midrc", "data_file", "injection", [
    "edge_crseriesfilerelatedtoimagingstudy",
    "edge_xaseriesfilerelatedtoimagingstudy",
    "edge_ptseriesfilerelatedtoimagingstudy",
    "edge_rfseriesfilerelatedtoimagingstudy",
    "edge_ctseriesfilerelatedtoimagingstudy",
    "edge_dxseriesfilerelatedtoimagingstudy",
    "edge_imagingstudyrelatedtocase",
    "edge_nmseriesfilerelatedtoimagingstudy",
    "edge_casememberofdataset",
    "edge_datasetperformedforproject",
    "edge_projectmemberofprogram"
])], indirect=True)
def test_create_props_from_json(translator):
    props = translator.parser.create_props_from_json(
        translator.parser.doc_type,
        [
            {'name': '_imaging_study_id', 'src': 'id', 'fn': 'set'},
            {'name': 'age_at_imaging', 'src': 'age_at_imaging', 'fn': 'set'},
            {'name': 'body_part_examined', 'src': 'body_part_examined', 'fn': 'set'},
            {'name': 'days_from_study_to_neg_covid_test', 'src': 'days_from_study_to_neg_covid_test', 'fn': 'set'},
            {'name': 'days_from_study_to_pos_covid_test', 'src': 'days_from_study_to_pos_covid_test', 'fn': 'set'},
            {'name': 'days_to_study', 'src': 'days_to_study', 'fn': 'set'},
            {'name': 'study_description', 'src': 'study_description', 'fn': 'set'},
            {'name': 'study_modality', 'src': 'study_modality', 'fn': 'set'},
            {'name': 'study_location', 'src': 'study_location', 'fn': 'set'},
            {'name': 'study_year', 'src': 'study_year', 'fn': 'set'},
            {'name': 'study_year_shifted', 'src': 'study_year_shifted', 'fn': 'set'},
            {'name': 'study_uid', 'src': 'study_uid', 'fn': 'set'}
        ]
    )
    types_to_check = {
        "_imaging_study_id": (str,),
        "age_at_imaging": (float, int, str),
        "body_part_examined": (list, str,),
        "days_from_study_to_neg_covid_test": (list, int),
        "days_from_study_to_pos_covid_test": (list, int),
        "days_to_study": (float, int, str),
        "study_description": (str, str),
        "study_modality": (list, str,),
        "study_location": (str, str,),
        "study_year": (int, str,),
        "study_year_shifted": (bool, str,),
        "study_uid": (str, str,)
    }
    actual_types = {}
    for p in props:
        actual_types[p.name] = p.type
    assert actual_types == types_to_check


@pytest.mark.schema_midrc
@pytest.mark.parametrize("translator", [("midrc", "imaging_study", "aggregation", [])], indirect=True)
def test_aggregate_with_nested_properties(translator):
    [expected_df] = get_dataframes_from_names(
        get_spark_session(translator.sc),
        "midrc",
        ["imaging_study__0_Translator.aggregate_nested_properties"]
    )
    actual_df = translator.aggregate_nested_properties()
    assert_dataframe_equality(
        expected_df, actual_df, get_node_id_name("imaging_study")
    )



@pytest.mark.schema_parent
@pytest.mark.parametrize("translator", [("parent", "participant", "aggregation", [])], indirect=True)
def test_nested_boolean_props(translator):
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
    print("Start boolean type testing")
    translator.update_types()
    participant_types = translator.nested_translator.parser.types["participant"]
    print("All of ES types")
    print(participant_types)
    assert participant_types["properties"]["participant_visits"]["properties"]["pregnancy_status"]["type"] == "keyword"
