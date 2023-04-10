import pytest
import tube.settings as config
from tests.dataframe_tests.util import (
    initialize_mappings,
    get_input_output_dataframes,
    get_spark_session,
    get_translator
)
from tube.utils.dd import init_dictionary

initialize_mappings("ibdgc")

@pytest.mark.parametrize("schema_context", ["ibdgc"], indirect=True)
def test_injection_parser(schema_context, spark_context):
    input_df, expected_df = get_input_output_dataframes(
        get_spark_session(spark_context),
        "ibdgc",
        "participant__0_Translator.translate_parent",
        "participant__0_Translator.get_direct_children"
    )
    dictionary, model = init_dictionary("abc")
    print(dictionary.schema)
