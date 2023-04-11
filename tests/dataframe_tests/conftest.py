import os
import pytest
import tube.settings as config

from unittest.mock import patch
from pyspark import SparkContext
from tube.utils.spark import make_spark_context
from tube.utils.dd import init_dictionary
from tube.etl.outputs.es.writer import Writer
from tube.etl.indexers.aggregation.new_translator import Translator as AggregationTranslator
from tube.etl.indexers.injection.new_translator import Translator as InjectionTranslator
from tests.dataframe_tests.util import mock_dictionary_url, initialize_mappings

TEST_DATA_HOME = "./tests/dataframe_tests/test_data"

@pytest.fixture(scope="session")
def spark_context() -> SparkContext:
    return make_spark_context(config)


@pytest.fixture()
def schema_context(request):
    patcher = patch(
        "dictionaryutils.load_schemas_from_url",
        return_value=mock_dictionary_url(request.param),
    )
    patcher.start()
    yield
    patcher.stop()


@pytest.fixture()
@patch(
    "dictionaryutils.load_schemas_from_url",
)
def translator(patch_dictionary_url, request):
    schema_name, mapping_name, indexer_type = request.param

    spark_context = make_spark_context(config)
    writer = Writer(spark_context, config)

    patch_dictionary_url.return_value = mock_dictionary_url(schema_name)
    dictionary, model = init_dictionary(config.DICTIONARY_URL)

    mapping = initialize_mappings(schema_name, mapping_name)

    hdfs_path = os.path.join(TEST_DATA_HOME, schema_name, "graphs")
    if indexer_type.lower() == "aggregation":
        return AggregationTranslator(
            spark_context, hdfs_path, writer, mapping, model, dictionary
        )
    elif indexer_type.lower() == "injection":
        return InjectionTranslator(
            spark_context, hdfs_path, writer, mapping, model, dictionary
        )
