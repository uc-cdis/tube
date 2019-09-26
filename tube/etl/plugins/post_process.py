import yaml

from tube.settings import PROJECT_TO_RESOURCE_PATH


def add_auth_resource_path(df):
    # add 'auth_resource_path' to resulting es document if 'project_id' exist
    if 'project_id' in df[1]:
        project_id = df[1]['project_id']
        if project_id is not None:
            s = project_id.split('-', 1)
            resource_path = PROJECT_TO_RESOURCE_PATH.get(s[1])
            if resource_path is None:
                df[1]['auth_resource_path'] = "/programs/{}/projects/{}".format(s[0], s[1])
            else:
                df[1]['auth_resource_path'] = resource_path
        else:
            df[1]['auth_resource_path'] = ''

    return df[0], df[1]
