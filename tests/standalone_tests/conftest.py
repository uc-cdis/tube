import os

from mock import patch
import pytest


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="function", autouse=True)
def mock_config_files(request):
    """
    This fixture automatically points Tube to default configuration files.
    To disable it for a test, mark it with: `@pytest.mark.noautosetup`.
    """
    if "noautosetup" in request.keywords._markers:
        yield
        return

    paths_mapping = request.param if hasattr(request, "param") else {}

    def find_paths_mock(file_name, *args, **kwargs):
        path = None
        if file_name in paths_mapping:
            path = paths_mapping[file_name]
        else:
            if file_name == "creds.json":
                path = os.path.join(CURRENT_DIR, "default_config_files/creds.json")
            elif file_name == "etlMapping.yaml":
                path = os.path.join(CURRENT_DIR, "default_config_files/etlMapping.yaml")
            elif file_name == "user.yaml":
                path = os.path.join(CURRENT_DIR, "default_config_files/user.yaml")
        if path:
            return [path]
        else:
            return []

    p = patch("tube.config_helper.find_paths", find_paths_mock)
    p.start()

    yield

    p.stop()


@pytest.fixture(scope="function")
def mock_configs(request):
    """
    Use this fixture to mock the contents of configuration files with empty data `{}`:
        def test_something(mock_configs):
            ...

    Optionally configure it by providing a mapping from file name to file contents:
        @pytest.mark.parametrize("mock_configs", [{"creds.json": {"db_database": "my_db"}}], indirect=True)
        def test_something(mock_configs):
            ...
    """
    paths_mapping = {}
    if hasattr(request, "param"):
        paths_mapping = request.param

    def load_json_mock(file_name, *args, **kwargs):
        data = None
        if file_name in paths_mapping:
            data = paths_mapping[file_name]
        if data:
            return data
        else:
            return {}

    p = patch("tube.config_helper.load_json", load_json_mock)
    p.start()

    yield

    p.stop()
