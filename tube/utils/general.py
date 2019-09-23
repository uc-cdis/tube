import yaml


def get_sql_to_hdfs_config(config):
    return {
        'input': {
            'jdbc': config['JDBC'],
            'username': config['DB_USERNAME'],
            'password': config['DB_PASSWORD'],
        },
        'output': config['HDFS_DIR']
    }


def list_to_file(lst, file_path):
    with open(file_path, 'w') as f:
        f.write('\n'.join(lst))


def get_resource_paths_from_yaml(useryaml_file):
    """
    Get all resource paths from user yaml file
    """
    if not useryaml_file:
        print("Can not find user.yaml file")
        return {}

    with open(useryaml_file, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            print("Can not read {}. Detail {}".format(useryaml_file, e))
            return {}
    
    results = {}
    for _, user in data.get("users", {}).iteritems():
        projects = user.get("projects", [])
        if not isinstance(projects, list):
            projects = [projects]
        for pr in projects:
            if "resource" in pr:
                results[pr.get("auth_id")] = pr["resource"]
    
    # if user_project_to_resource is in user yaml
    json_data = data.get("authz", data.get("rbac"))
    if json_data:
        get_resource_path_from_json(results, json_data)
    return results


def get_resource_path_from_json(results, json_data):
    for project in json_data.get('user_project_to_resource', {}):
        results[project] = json_data['user_project_to_resource'][project]
    resources = json_data.get('resources')
    if resources is None:
        return
    for it in resources:
        it_name = it.get('name')
        if it.get('name') is None:
            return
        if it_name != 'programs':
            continue
        programs = it.get('subresources')
        if programs is None:
            return
        for program in programs:
            program_name = program.get('name')
            if program_name is None:
                return
            projects = program.get('subresources')
            for project in projects:
                project_name = project.get('name')
                results[project_name] = '/programs/{0}/projects/{1}'.\
                    format(program_name, project_name)
