import os
import time

import pytest

from tube import config_helper


# WORKSPACE == Jenkins workspace
TEST_ROOT = (
    os.getenv("WORKSPACE", os.getenv("XDG_RUNTIME_DIR", "/tmp"))
    + "/test_config_helper/"
    + str(int(time.time()))
)
APP_NAME = "test_config_helper"
TEST_JSON = """
{
  "a": "A",
  "b": "B",
  "c": "C"
}
"""
TEST_FILENAME = "bla.json"

config_helper.XDG_DATA_HOME = TEST_ROOT


@pytest.fixture
def setup():
    test_folder = TEST_ROOT + "/gen3/" + APP_NAME
    if not os.path.exists(test_folder):
        os.makedirs(test_folder)
    with open(test_folder + "/" + TEST_FILENAME, "w") as writer:
        writer.write(TEST_JSON)


def test_find_path(setup):
    expected_path = TEST_ROOT + "/gen3/" + APP_NAME + "/" + TEST_FILENAME
    output_path = config_helper.find_path(TEST_FILENAME, APP_NAME)
    assert os.path.exists(expected_path)
    assert output_path == expected_path


def test_load_json(setup):
    data = config_helper.load_json(TEST_FILENAME, APP_NAME)
    for key in ["a", "b", "c"]:
        assert data[key] == key.upper()
