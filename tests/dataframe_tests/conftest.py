import pytest
import tube.settings as config

from unittest.mock import patch
from pyspark import SparkContext
from tube.utils.spark import make_spark_context


@pytest.fixture(scope="session")
def spark_context() -> SparkContext:
    return make_spark_context(config)


@pytest.fixture
def schema_context():
    return patch("dictionaryutils.load_schemas_from_url")
