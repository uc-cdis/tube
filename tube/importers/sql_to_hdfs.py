import os
import subprocess
import psycopg2


def get_all_tables(pydbc):
    conn = psycopg2.connect(pydbc)
    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = list(map(lambda i: i[0], cursor.fetchall()))
    cursor.close()
    conn.close()
    return tables


def import_table_from_sql(tb, jdbc, username, password, output_dir):
    myenv = os.environ.copy()
    execs = [
        'sqoop', 'import',
        '--direct', '--connect', jdbc,
        '--username', username,
        '--password', password,
        '--table', tb,
        '--m', '1',
        '--target-dir', output_dir + '/{}'.format(tb),
        '--map-column-java', '_props=String,acl=String,_sysan=String'
    ]
    sp = subprocess.Popen(execs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return sp


def generate_sp(sp):
    line = sp.stdout.readline()
    while line != '':
        yield line
        line = sp.stdout.readline()


def generate_import(tables, config):
    input = config['input']
    output = config['output']

    for tb in tables:
        if 'node' not in tb and 'edge' not in tb:
            continue
        yield tb + '<br/>\n'
        sp = import_table_from_sql(
                tb,
                input['jdbc'],
                input['username'],
                input['password'],
                output)

        line = sp.stdout.readline()
        while line != '':
            yield line + '<br/>\n'
            line = sp.stdout.readline()
            if line == '':
                line = sp.stderr.readline()
