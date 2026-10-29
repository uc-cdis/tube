import pytest

from unittest.mock import patch

from tests.util import mock_dictionary_url, initialize_mappings
from tube.etl.indexers.injection.parser import Parser as InjectionParser
import tube.settings as config
from tube.utils.dd import init_dictionary

@pytest.fixture(scope="function")
@patch("tube.etl.indexers.injection.parser.Parser.get_edges_having_data")
@patch(
    "dictionaryutils.load_schemas_from_url",
)
def test_create_prop_from_json():
    """
    Unit test for parser to check if the property created is correct.
    """
    from tube.etl.indexers.base.parser import Parser

    dictionary_url_patcher = patch(
        "dictionaryutils.load_schemas_from_url",
        return_value=mock_dictionary_url("midrc"),
    )
    dictionary_url_patcher.start()
    dictionary, model = init_dictionary("nor_used_url")

    for e in model.Edge.get_subclasses():
        print(e.__tablename__)

    mapping = initialize_mappings("midrc", "data_file")
    parser = InjectionParser(mapping, model, dictionary)
    for prop in parser.props:
        assert prop.type is not None


def test_select_widest_type():
    """Props without type info (not in the dictionary) default to str instead of raising."""
    from tube.etl.indexers.base.parser import Parser

    assert Parser.select_widest_type(None) is str
    assert Parser.select_widest_type([]) is str
    assert Parser.select_widest_type([str, int]) is int
    assert Parser.select_widest_type([int, float]) is float
    assert Parser.select_widest_type([bool]) is bool
