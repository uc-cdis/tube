import os
import pytest

from tube.etl.outputs.es.settings_util import (
    build_properties,
    get_analyzer_settings,
    get_settings,
)
import tube.config as config


def test_settings():
    settings = get_settings(config)
    assert len(settings.keys()) == 1
    assert next(iter(settings.keys())) == "settings"


def test_analyzer_settings():
    assert get_analyzer_settings(config), "broken"


def test_build_properties():
    test_input = {"subject_id": [str], "file_size": [float]}
    expected_output = {
        "subject_id": {
            "type": "keyword",
            "fields": {
                "analyzed": {
                    "type": "text",
                    "analyzer": "ngram_analyzer",
                    "search_analyzer": "search_analyzer",
                }
            },
        },
        "file_size": {"type": "float"},
        "node_id": {"type": "keyword"},
    }

    assert build_properties(config, test_input) == expected_output
