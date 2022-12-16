def add_auth_resource_path_mapping(doc_type, types):
    types[doc_type]["properties"]["auth_resource_path"] = {
        "type": "keyword",
        "fields": {"analyzed": {"type": "text"}},
    }
    types[doc_type]["properties"]["project_id"] = {
        "type": "keyword",
        "fields": {"analyzed": {"type": "text"}},
    }
    return types
