import pytest
import tube.settings as config

from typing import Generator
from pyspark import SparkContext
from tube.utils.spark import make_spark_context


@pytest.fixture(scope="session")
def spark_context() -> Generator[SparkContext, None, None]:
    with make_spark_context(config) as spark_context:
        yield spark_context
