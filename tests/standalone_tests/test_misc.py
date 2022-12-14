def test_add_auth_resource_path_mapping():
    """
    Regression test that checks that `add_auth_resource_path_mapping` always adds
    the `auth_resource_path` and `project_id` properties, instead of only adding
    `auth_resource_path` when the properties already include `project_id`.
    """
    from tube.etl.plugins import add_auth_resource_path_mapping

    doc_type = "subject"
    types = {doc_type: {"properties": {}}}
    returned_types = add_auth_resource_path_mapping(doc_type, types)
    assert returned_types == {
        "subject": {
            "properties": {
                "auth_resource_path": {
                    "type": "keyword",
                    "fields": {"analyzed": {"type": "text"}},
                },
                "project_id": {
                    "type": "keyword",
                    "fields": {"analyzed": {"type": "text"}},
                },
            }
        }
    }
