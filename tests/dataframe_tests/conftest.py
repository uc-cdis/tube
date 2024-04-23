import os
import pytest
import tube.settings as config

from unittest.mock import patch
from tube.utils.spark import make_spark_context
from tube.utils.dd import init_dictionary
from tube.etl.indexers.aggregation.new_translator import (
    Translator as AggregationTranslator,
)
from tube.etl.indexers.injection.new_translator import Translator as InjectionTranslator
from tests.util import mock_dictionary_url, initialize_mappings

TEST_DATA_HOME = "./tests/dataframe_tests/test_data"


@pytest.fixture(scope="function")
@patch("tube.etl.indexers.injection.parser.Parser.get_edges_having_data")
@patch(
    "dictionaryutils.load_schemas_from_url",
)
def translator(patch_dictionary_url, mock_get_edges_having_data, request):
    schema_name, mapping_name, indexer_type, edges_having_data = request.param

    spark_context = make_spark_context(config)

    print(schema_name)
    dictionary_url_patcher = patch(
        "dictionaryutils.load_schemas_from_url",
        return_value=mock_dictionary_url(schema_name),
    )
    dictionary_url_patcher.start()
    dictionary, model = init_dictionary("nor_used_url")

    for e in model.Edge.get_subclasses():
        print(e.__tablename__)

    mapping = initialize_mappings(schema_name, mapping_name)

    hdfs_path = os.path.join(TEST_DATA_HOME, schema_name, "graphs")
    translator = None
    if indexer_type.lower() == "aggregation":
        translator = AggregationTranslator(
            spark_context, hdfs_path, None, mapping, model, dictionary
        )
    elif indexer_type.lower() == "injection":
        edges_having_data_patcher = patch(
            "tube.etl.indexers.injection.parser.Parser.get_edges_having_data",
            return_value=edges_having_data,
        )
        edges_having_data_patcher.start()
        translator = InjectionTranslator(
            spark_context, hdfs_path, None, mapping, model, dictionary
        )
        edges_having_data_patcher.stop()
    dictionary_url_patcher.stop()
    return translator
