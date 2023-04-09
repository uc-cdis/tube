import tube.settings as config
import pytest
from unittest.mock import patch
from tests.dataframe_tests.util import (
    get_spark_session,
    initialize_mappings,
    get_translator,
    assert_dataframe_equality,
    get_input_output_dataframes
)
from tube.utils.general import get_node_id_name

initialize_mappings("ibdgc")

def test_injection_parser(schema_context):
    pass
