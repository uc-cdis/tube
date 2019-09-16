import yaml

from tube.settings import PROJECT_TO_RESOURCE_PATH


def add_auth_resource_path(df):
    # add 'auth_resource_path' to resulting es document if 'project_id' exist
    if 'project_id' in df[1]:
        project_id = df[1]['project_id']
        if project_id is not None:
            program_name, project_code = project_id.split('-', 1)
            resource_path = PROJECT_TO_RESOURCE_PATH[project_code]
            if not resource_path:
                print("WARNING: Can not get resource path from user.yaml for project code {}".format(project_code))
                df[1]['auth_resource_path'] = "/programs/{}/projects/{}".format(program_name, project_code)
            else:
                df[1]['auth_resource_path'] = resource_path
        else:
            df[1]['auth_resource_path'] = ''

    return df[0], df[1]
