import subprocess
import psycopg2
from tube.utils.spark import make_sure_hdfs_path_exist
from tube.utils.general import list_to_file, get_sql_to_hdfs_config


class SqlToHDFS(object):
    def __init__(self, config, formatter):
        self.config = config
        self.formatter = formatter

    def get_all_tables(self):
        conn = psycopg2.connect(self.config.PYDBC)
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = list([i[0] for i in cursor.fetchall()])
        list_to_file(tables, self.config.LIST_TABLES_FILES)
        cursor.close()
        conn.close()
        return tables

    @classmethod
    def import_all_tables_from_sql(cls, jdbc, username, password, output_dir, m):
        execs = [
            'sqoop', 'import-all-tables',
            '--direct', '--connect', jdbc,
            '--username', username,
            '--password', password,
            '--m', '{}'.format(m),
            '--warehouse-dir', output_dir,
            '--outdir', 'temp',
            '--enclosed-by', '"',
            '--exclude-tables',
            'transaction_documents,transaction_logs,transaction_snapshots,_voided_edges,_voided_nodes',
            '--map-column-java', '_props=String,acl=String,_sysan=String'
        ]
        sp = subprocess.Popen(execs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return sp

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
        output = make_sure_hdfs_path_exist(config['output'])

        sp = SqlToHDFS.import_all_tables_from_sql(
                config['input']['jdbc'],
                config['input']['username'],
                config['input']['password'],
                output, self.config.PARALLEL_JOBS
            )

        line = sp.stdout.readline().decode()
        while line != '':
            yield self.formatter.format_line(line)
            line = sp.stdout.readline().decode()

    def generate_import_all_tables_gradually(self):
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
