import pytest
import tube.settings as config

from unittest.mock import patch
from pyspark import SparkContext
from tube.utils.spark import make_spark_context
from tests.dataframe_tests.util import mock_dictionary_url


@pytest.fixture(scope="session")
def spark_context() -> SparkContext:
    return make_spark_context(config)


@pytest.fixture(scope="function")
def schema_context(request):
    print("In schema context fixture")
    print(request.__dict__)
    print(dir(request))
    patcher = patch(
        "dictionaryutils.load_schemas_from_url",
        return_value=mock_dictionary_url(request.param)
    )
    patcher.start()
    yield
    patcher.stop()
