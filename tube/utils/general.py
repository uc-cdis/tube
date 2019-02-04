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
