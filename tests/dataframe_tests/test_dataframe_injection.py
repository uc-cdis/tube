import pytest
from tests.dataframe_tests.util import (
    get_spark_session,
    assert_dataframe_equality,
    get_dataframes_from_names,
)
from tube.utils.general import get_node_id_name

@pytest.mark.schema_ibdgc
@pytest.mark.parametrize("translator", [("ibdgc", "file", "injection", [
        "edge_qcworkflowperformedonrawsnpgenotype", "edge_ebf408fc_sufidafrcomeco", "edge_d0f931be_alredafrcomeco",
        "edge_diagnosisdescribesvisit", "edge_supplementaryfiledatafromproject", "edge_4c2316bf_alcowopeonsualre",
        "edge_centercontributedtoproject", "edge_f882ed04_refidafrcomeco", "edge_aed49361_madilopeatvi",
        "edge_exposureperformedatvisit", "edge_64a2ed15_suunredafrregr", "edge_0b990528_alwopeonsualre",
        "edge_demographicdescribesparticipant", "edge_aliquotderivedfromsample", "edge_publicationreferstoproject",
        "edge_bb548516_exinmapeatvi", "edge_projectmemberofprogram", "edge_5db8cb04_gevaindefrsigeva",
        "edge_readgroupderivedfromaliquot", "edge_participantrecruitedatcenter", "edge_keyworddescribeproject",
        "edge_1eebce42_anfidafrcomeco", "edge_6dd7191b_diacantrpeatvi", "edge_aliasidentifiesparticipant",
        "edge_sampleididentifiessample", "edge_familyhistoryperformedatvisit", "edge_32dd5475_sigevadafrcomeco",
        "edge_561d0dc7_sufidafrpu", "edge_6fadb507_gemucawopeonsualre", "edge_9c338d5d_rasngedafrcomeco",
        "edge_diagnosisdescribesparticipant", "edge_acknowledgementcontributetoproject",
        "edge_visitdescribesparticipant", "edge_54e02e8f_sualredafrcomeco", "edge_eeedc770_sufidafrcomeco",
        "edge_pubertalstageperformedatvisit", "edge_176f8285_prmedafrcomeco", "edge_9197510c_comecodafrpr",
        "edge_surgeryperformedatvisit", "edge_samplederivedfromparticipant"
    ])], indirect=True)
def test_collect_collecting_child(translator):
    """
    Test function which makes dataframe contains data of all collecting nodes.
    - no input dataframe
    - aggregated dataframe containing all collecting nodes' data
    :param translator:
    :return:
    """
    [expected_df] = get_dataframes_from_names(
        get_spark_session(translator.sc),
        "ibdgc",
        ["file__0_Translator.collect_collecting_child__collected_collecting_dfs__aligned_reads"]
    )
    collecting_nodes = ["participant", "publication", "sample","aligned_reads", "aliquot", "annotation_file", "center",
                        "core_metadata_collection", "raw_snp_genotype", "read_group", "reference_file",
                        "simple_germline_variation", "submitted_aligned_reads", "submitted_unaligned_reads",
                        "summary_file", "visit"]

    expected_collected_collecting_dfs = {}
    for n in collecting_nodes:
        [expected_df] = get_dataframes_from_names(
            get_spark_session(translator.sc),
            "ibdgc",
            [f"file__0_Translator.collect_collecting_child__collected_collecting_dfs__{n}"]
        )
        expected_collected_collecting_dfs[n] = expected_df

    collected_collecting_dfs = translator.join_program_to_project()
    translator.merge_collectors(collected_collecting_dfs)
    print(f"Collected collecting dfs: {collected_collecting_dfs}")
    for n in collecting_nodes:
        print(f"Node: {n}")
        print(f"Expected collecting df: {expected_collected_collecting_dfs.get(n)}")
        print(f"Collected collecting df: {collected_collecting_dfs.get(n)}")
        assert_dataframe_equality(
            expected_collected_collecting_dfs.get(n), collected_collecting_dfs.get(n), get_node_id_name(n)
        )

@pytest.mark.schema_jcoin
@pytest.mark.parametrize("translator", [("jcoin", "file", "injection", [
        "edge_0029a6d1_admedapeatfoup", "edge_122d81fe_orrepeatfoup", "edge_16158be2_imoumepeatfoup",
        "edge_176f8285_prmedafrcomeco", "edge_21fa3fe9_riofhaancopeatfoup", "edge_22014405_juinqupeatfoup",
        "edge_2d0f7d59_moqudepa", "edge_3b95de47_orclstpeatfoup", "edge_5feaa397_riofhaancopeattipo",
        "edge_638e0a47_trprqupeatfoup", "edge_67a7e2d0_trprpeattipo", "edge_87949a9c_stattomopeatfoup",
        "edge_9197510c_comecodafrpr", "edge_a3b21dc3_utsequpeatfoup", "edge_acknowledgementcontributetoproject",
        "edge_centerperformedatfollowup", "edge_d200dca4_seadevreatpr", "edge_d56b01d7_badepeatfoup",
        "edge_d68708cb_alreindafrcomeco", "edge_d8606352_utsepeattipo", "edge_demographicdescribesparticipant",
        "edge_e7e94b83_juinpeattipo", "edge_e92eeb59_dehopeattipo", "edge_enrollmentdescribesparticipant",
        "edge_environmentrecruitedatcenter", "edge_f4044444_debadepa", "edge_f882ed04_refidafrcomeco",
        "edge_followupdescribesparticipant", "edge_healthperformedatfollowup", "edge_keyworddescribeproject",
        "edge_mouduseperformedatparticipant", "edge_organizationrecruitedatcenter", "edge_oudperformedatfollowup",
        "edge_participantrecruitedatprotocol", "edge_practitionerrecruitedatcenter", "edge_projectmemberofprogram",
        "edge_promisperformedatfollowup", "edge_promisperformedattimepoint", "edge_protocolcontributedtoproject",
        "edge_publicationreferstoproject", "edge_recoveryperformedatfollowup", "edge_substanceuseperformedatfollowup",
        "edge_substanceuseperformedattimepoint", "edge_systemrecruitedatcenter", "edge_timepointdescribesparticipant"
    ])], indirect=True)
def test_get_leaves(translator):
    """
    Test get_leaves function to ensure that the function working correctly.
    This also include the test case where there are two properties
    with the same source field in dictionary but being renamed in the index
    - input collected collecting node's dataframe
    - expected final dataframe which have all leaf nodes
    :param translator:
    :return:
    """
    collecting_nodes = ["core_metadata_collection", "reference_file"]

    input_collected_collecting_dfs = {}
    for n in collecting_nodes:
        [input_df] = get_dataframes_from_names(
            get_spark_session(translator.sc),
            "jcoin",
            [f"file__0_Translator.collect_collecting_child__collected_collecting_dfs__{n}"]
        )
        input_collected_collecting_dfs[n] = input_df
    [expected_collect_leaf_final] = get_dataframes_from_names(
        get_spark_session(translator.sc),
        "jcoin",
        ["file__0_Translator.translate__collected_leaf_dfs"]
    )

    collected_leaf_dfs = {}
    translator.get_leaves(input_collected_collecting_dfs, collected_leaf_dfs)
    translator.merge_collectors(collected_leaf_dfs)
    print(f"Collected collecting dfs: {collected_leaf_dfs}")
    print(collected_leaf_dfs.get("final").schema)
    assert_dataframe_equality(
        expected_collect_leaf_final, collected_leaf_dfs.get("final"), "_file_id"
    )

@pytest.mark.schema_midrc
@pytest.mark.parametrize("translator", [("midrc", "imaging_data_file", "injection", [
        "edge_crseriesfilerelatedtoimagingstudy", "edge_dxseriesfilerelatedtoimagingstudy",
        "edge_mrseriesfilerelatedtoimagingstudy", "edge_ctseriesfilerelatedtoimagingstudy",
        "edge_0c3e44da_crsefidafrcomeco", "edge_a4f04f84_dxsefidafrcomeco", "edge_d04a5ba2_mrsefidafrcomeco",
        "edge_0b8a28a9_ctsefidafrcomeco", "edge_9197510c_comecodafrpr", "edge_casememberofdataset",
        "edge_ctscanrelatedtoimagingstudy", "edge_ctscanrelatedtosubject", "edge_ctseriesrelatedtoctscan",
        "edge_datareleasedescribesroot", "edge_datasetperformedforproject", "edge_ef9975fc_cotofidafrimex",
        "edge_imagingstudyrelatedtocase", "edge_projectmemberofprogram",
    ])], indirect=True)
def test_flatten_nested_list(translator):
    """
    Test to ensure the created dataframe will not contains any array being nested in another array
    - input dataframe with leaf nodes containing nested list
    - expected dataframe with leaf nodes no longer containing nested list
    :param translator:
    :return:
    """
    [collected_leaf_df, final_df] = get_dataframes_from_names(
        get_spark_session(translator.sc),
        "midrc",
        [
            "imaging_data_file__0_Translator.translate__collected_leaf_dfs",
            "imaging_data_file__1_Translator.translate_final__translate_final"
        ]
    )
    aggregating_props = translator.get_aggregating_props()
    actual_final_df = translator.flatten_nested_list(collected_leaf_df, aggregating_props)
    print(f"Actual df: {actual_final_df}")
    assert_dataframe_equality(
        final_df, actual_final_df, "_imaging_data_file_id"
    )
