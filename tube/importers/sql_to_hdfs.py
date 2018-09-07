import subprocess
import psycopg2
from tube.utils import list_to_file, get_sql_to_hdfs_config, make_sure_hdfs_path_exist


class SqlToHDFS(object):
    def __init__(self, config, formatter):
        self.config = config
        self.formatter = formatter

    def get_all_tables(self):
        conn = psycopg2.connect(self.config.PYDBC)
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = list(map(lambda i: i[0], cursor.fetchall()))
        list_to_file(tables, self.config.LIST_TABLES_FILES)
        cursor.close()
        conn.close()
        return tables

    @classmethod
    def import_table_from_sql(cls, tb, jdbc, username, password, output_dir, m):
        optional_fields = 'node_id=String,' if tb.startswith('node_') else 'src_id=String,dst_id=String,'
        execs = [
            'sqoop', 'import',
            '--direct', '--connect', jdbc,
            '--username', username,
            '--password', password,
            '--table', tb,
            '--m', '{}'.format(m),
            '--target-dir', output_dir + '/{}'.format(tb),
            '--outdir', 'temp',
            '--enclosed-by', '"',
            '--map-column-java', '_props=String,acl=String,_sysan=String,{}'.format(optional_fields)
        ]
        sp = subprocess.Popen(execs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return sp

    def generate_import_all_tables(self):
        config = get_sql_to_hdfs_config(self.config.__dict__)
        tables = self.get_all_tables()
        output = make_sure_hdfs_path_exist(config['output'])

        for tb in tables:
            if not tb.startswith('node') and not tb.startswith('edge'):
                continue
            yield self.formatter.format_line(tb)
            sp = SqlToHDFS.import_table_from_sql(
                tb,
                config['input']['jdbc'],
                config['input']['username'],
                config['input']['password'],
                output, self.config.PARALLEL_JOBS
            )

            line = sp.stdout.readline()
            while line != '':
                yield self.formatter.format_line(line)
                line = sp.stdout.readline()
                if line == '':
                    line = sp.stderr.readline()
