import pytest
from tests.dataframe_tests.util import (
    get_spark_session,
    assert_dataframe_equality,
    get_input_output_dataframes,
)
from tube.utils.general import get_node_id_name

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
    input_df, expected_df = get_input_output_dataframes(
        get_spark_session(translator.sc),
        "ibdgc",
        None,
        "file__0_Translator.collect_collecting_child__collected_collecting_dfs__aligned_reads"
    )
    collecting_nodes = ["participant", "publication", "sample","aligned_reads", "aliquot", "annotation_file", "center",
                        "core_metadata_collection", "raw_snp_genotype", "read_group", "reference_file",
                        "simple_germline_variation", "submitted_aligned_reads", "submitted_unaligned_reads",
                        "summary_file", "visit"]

    expected_collected_collecting_dfs = {}
    for n in collecting_nodes:
        input_df, expected_df = get_input_output_dataframes(
            get_spark_session(translator.sc),
            "ibdgc",
            None,
            f"file__0_Translator.collect_collecting_child__collected_collecting_dfs__{n}"
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
