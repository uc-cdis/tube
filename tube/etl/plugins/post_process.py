def add_auth_resource_path(df):
    # add 'auth_resource_path' to resulting es document if 'project_id' exist
    if 'project_id' in df[1]:
        project_id = df[1]['project_id']
        if project_id is not None:
            program_name, project_code = project_id.split('-', 1)
            df[1]['auth_resource_path'] = '/programs/{}/projects/{}'.format(program_name, project_code)
        else:
            df[1]['auth_resource_path'] = ''

    return df[0], df[1]
