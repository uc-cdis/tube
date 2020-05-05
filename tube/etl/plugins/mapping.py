def add_auth_resource_path_mapping(types):
    if "project_id" in types:
        types["auth_resource_path"] = (str, 0)
    return types
