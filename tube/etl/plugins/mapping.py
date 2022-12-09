def add_auth_resource_path_mapping(doc_type, types):
    properties = types.get(doc_type).get("properties")
    types[doc_type]["properties"]["auth_resource_path"] = {
        "type": "keyword",
        "fields": {"analyzed": {"type": "text"}},
    }
    types[doc_type]["properties"]["project_id"] = {
        "type": "keyword",
        "fields": {"analyzed": {"type": "text"}},
    }
    return types
