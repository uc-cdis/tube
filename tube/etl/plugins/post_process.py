import yaml

from tube.settings import USERYAML_FILE


def _get_resource_path_from_yaml(project):
    """
    Get resource path from user yaml file
    """
    if not USERYAML_FILE:
        print("Can not find user.yaml file")
        return ""
    
    with open(USERYAML_FILE, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            print("Can not read {}. Detail {}".format(USERYAML_FILE, e))
            return ""
    
    if project in data.get("user_project_to_resource", {}):
        return data["user_project_to_resource"][project]

    for _, user in data.get("users", {}).iteritems():
        projects = user.get("projects", [])
        if not isinstance(projects, list):
            projects = [projects]
        for pr in projects:
            if pr.get("auth_id") == project:
                if "resource" in pr:
                    return pr["resource"]
    return ""


def add_auth_resource_path(df):
    # add 'auth_resource_path' to resulting es document if 'project_id' exist
    if 'project_id' in df[1]:
        project_id = df[1]['project_id']
        if project_id is not None:
            program_name, project_code = project_id.split('-', 1)
            df[1]['auth_resource_path'] = _get_resource_path_from_yaml(project_code)
        else:
            df[1]['auth_resource_path'] = ''

    return df[0], df[1]
