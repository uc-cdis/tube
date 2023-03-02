import pytest
import tube.settings as config

from typing import Generator
from pyspark import SparkContext
from tube.utils.spark import make_spark_context


@pytest.fixture(scope="session")
def spark_context() -> SparkContext:
    return make_spark_context(config)
