def get_sql_to_hdfs_config(config):
    return {
        'input': {
            'jdbc': config['JDBC'],
            'username': config['DB_USERNAME'],
            'password': config['DB_PASSWORD'],
        },
        'output': config['HDFS_DIR']
    }
