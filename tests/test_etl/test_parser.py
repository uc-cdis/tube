import pytest
from tube.etl.indexers.injection.parser import Parser


@pytest.fixture
def test_injecting_parser():
    parser = Parser()
