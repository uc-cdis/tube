import yaml

from settings import USERYAML_FILE


def _get_resource_path_from_yaml(project):
    """
    Get resource path from user yaml file
    """

    if not USERYAML_FILE:
        return ''
    with open(USERYAML_FILE, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            print("Can not read {}. Detail {}", USERYAML_FILE, e)
            return ''
    try:
        for _, v in data['users'].iteritems():
            projects = v['projects']
            if not isinstance(projects, list):
                projects = [projects]
            for pr in projects:
                if pr['auth_id'] == project:
                    return pr['resource']
    except KeyError as e:
        return ''
    
    return ''


def add_auth_resource_path(df):
    # add 'auth_resource_path' to resulting es document if 'project_id' exist
    if 'project_id' in df[1]:
        project_id = df[1]['project_id']
        if project_id is not None:
            program_name, project_code = project_id.split('-', 1)
            resource_path = _get_resource_path_from_yaml(project_code)
            df[1]['auth_resource_path'] = resource_path
        else:
            df[1]['auth_resource_path'] = ''

    return df[0], df[1]
