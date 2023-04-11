import pytest
from tests.dataframe_tests.util import (
    initialize_mappings,
    get_input_output_dataframes,
    get_spark_session,
)
from tube.utils.dd import init_dictionary

@pytest.mark.parametrize("translator", [("midrc", "file", "injection")], indirect=True)
def test_injection_parser(translator):
    input_df, expected_df = get_input_output_dataframes(
        get_spark_session(translator.sc),
        "ibdgc",
        "participant__0_Translator.translate_parent",
        "participant__0_Translator.get_direct_children"
    )
    dictionary, model = init_dictionary("abc")
    print(dictionary.schema)
