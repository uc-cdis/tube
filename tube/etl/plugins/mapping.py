def add_auth_resource_path_mapping(doc_type, types):
    properties = types.get(doc_type).get("properties")
    if "project_id" in types:
        types["auth_resource_path"] = (str, 0)
    return types
