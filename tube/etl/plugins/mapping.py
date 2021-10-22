def add_auth_resource_path_mapping(doc_type, types):
    properties = types.get(doc_type).get("properties")
    if "project_id" in properties:
        types[doc_type]["properties"]["auth_resource_path"] = {
            "type": "keyword",
            "fields": {"analyzed": {"type": "text"}},
        }
    return types
