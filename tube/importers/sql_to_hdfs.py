import subprocess

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from tube.utils.spark import make_spark_context, make_sure_hdfs_path_exist
from tube.utils.general import list_to_file, get_sql_to_hdfs_config


EXCLUDED_TABLES = {
    "transaction_documents",
    "transaction_logs",
    "transaction_snapshots",
    "_voided_edges",
    "_voided_nodes",
}


class SqlToHDFS(object):
    def __init__(self, config, formatter):
        self.config = config
        self.formatter = formatter

    def get_all_tables(self):
        conn = None

        try:
            if self.config.DB_USE_SSL:
                conn = psycopg2.connect(
                    dbname=self.config.DB_DATABASE,
                    user=self.config.DB_USERNAME,
                    password=self.config.DB_PASSWORD,
                    host=self.config.DB_HOST,
                    port=self.config.DB_PORT,
                    sslmode="require",
                )
            else:
                conn = psycopg2.connect(self.config.PYDBC)

            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            )

            tables = [
                row[0]
                for row in cursor.fetchall()
                if row[0] not in EXCLUDED_TABLES
            ]

            list_to_file(
                tables,
                self.config.LIST_TABLES_FILES,
            )

            cursor.close()

            return tables

        finally:
            if conn is not None:
                conn.close()

    @classmethod
    def import_all_tables_from_sql(
        cls,
        jdbc,
        username,
        password,
        output_dir,
        m,
    ):
        execs = [
            "sqoop",
            "import-all-tables",
            "--direct",
            "--connect",
            jdbc,
            "--username",
            username,
            "--password",
            password,
            "--m",
            "{}".format(m),
            "--warehouse-dir",
            output_dir,
            "--outdir",
            "temp",
            "--enclosed-by",
            '"',
            "--exclude-tables",
            (
                "transaction_documents,"
                "transaction_logs,"
                "transaction_snapshots,"
                "_voided_edges,"
                "_voided_nodes"
            ),
            "--map-column-java",
            "_props=String,acl=String,_sysan=String",
        ]

        sp = subprocess.Popen(
            execs,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        return sp

    @classmethod
    def import_table_from_sql(
        cls,
        tb,
        jdbc,
        username,
        password,
        output_dir,
        m,
    ):
        optional_fields = (
            "node_id=String,"
            if tb.startswith("node_")
            else "src_id=String,dst_id=String,"
        )

        execs = [
            "sqoop",
            "import",
            "--direct",
            "--connect",
            jdbc,
            "--username",
            username,
            "--password",
            password,
            "--table",
            tb,
            "--m",
            "{}".format(m),
            "--target-dir",
            output_dir + "/{}".format(tb),
            "--outdir",
            "temp",
            "--enclosed-by",
            '"',
            "--map-column-java",
            "_props=String,acl=String,_sysan=String,{}".format(
                optional_fields
            ),
        ]

        sp = subprocess.Popen(
            execs,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        return sp

    def _jdbc_properties(self):
        return {
            "user": self.config.DB_USERNAME,
            "password": self.config.DB_PASSWORD,
            "driver": "org.postgresql.Driver",
            "fetchsize": str(self.config.JDBC_FETCH_SIZE),
        }

    def _read_table_with_jdbc(
        self,
        spark,
        table_name,
    ):
        return spark.read.jdbc(
            url=self.config.JDBC,
            table='"{}"'.format(table_name),
            properties=self._jdbc_properties(),
        )

    def _write_sqoop_compatible_text(
        self,
        df,
        output_path,
    ):
        """
        Preserve the format expected by the existing Tube parsers.

        Sqoop currently writes rows equivalent to:

            "field1","field2","field3",...

        Tube later reads them with Spark textFile() and ast.literal_eval().
        """

        string_df = df.select(
            *[
                col(column_name)
                .cast("string")
                .alias(column_name)
                for column_name in df.columns
            ]
        )

        (
            string_df.write
            .mode("overwrite")
            .option("header", "false")
            .option("quote", '"')
            .option("escape", '"')
            .option("quoteAll", "true")
            .option("nullValue", "null")
            .csv(output_path)
        )

    def generate_import_all_tables_jdbc(self):
        sc = None
        spark = None

        try:
            sc = make_spark_context(self.config)

            spark = SparkSession.builder.getOrCreate()

            make_sure_hdfs_path_exist(
                self.config.HDFS_DIR,
                sc=sc,
            )

            tables = self.get_all_tables()

            yield self.formatter.format_line(
                "Spark JDBC import: {} tables".format(
                    len(tables)
                )
            )

            for table_name in tables:
                if not (
                    table_name.startswith("node_")
                    or table_name.startswith("edge_")
                ):
                    continue

                yield self.formatter.format_line(
                    "Importing {} with Spark JDBC".format(
                        table_name
                    )
                )

                df = self._read_table_with_jdbc(
                    spark,
                    table_name,
                )

                output_path = "{}/{}".format(
                    self.config.HDFS_DIR.rstrip("/"),
                    table_name,
                )

                self._write_sqoop_compatible_text(
                    df,
                    output_path,
                )

                yield self.formatter.format_line(
                    "Finished {}".format(table_name)
                )

        finally:
            if spark is not None:
                spark.stop()
            SparkSession._instantiatedSession = None
            SparkSession._activeSession = None            

    def generate_import_all_tables_sqoop(self):
        config = get_sql_to_hdfs_config(
            self.config.__dict__
        )

        output = make_sure_hdfs_path_exist(
            config["output"]
        )

        sp = SqlToHDFS.import_all_tables_from_sql(
            config["input"]["jdbc"],
            config["input"]["username"],
            config["input"]["password"],
            output,
            self.config.PARALLEL_JOBS,
        )

        line = sp.stdout.readline().decode()

        while line != "":
            yield self.formatter.format_line(line)
            line = sp.stdout.readline().decode()

        return_code = sp.wait()

        if return_code != 0:
            raise RuntimeError(
                "Sqoop import failed with exit code {}".format(
                    return_code
                )
            )

    def generate_import_all_tables(self):
        if (
            self.config.DB_IMPORT_MODE.lower()
            == "spark-jdbc"
        ):
            yield from self.generate_import_all_tables_jdbc()
            return

        yield from self.generate_import_all_tables_sqoop()

    def generate_import_all_tables_gradually(self):
        config = get_sql_to_hdfs_config(
            self.config.__dict__
        )

        tables = self.get_all_tables()

        output = make_sure_hdfs_path_exist(
            config["output"]
        )

        for tb in tables:
            if not tb.startswith("node") and not tb.startswith("edge"):
                continue

            yield self.formatter.format_line(tb)

            sp = SqlToHDFS.import_table_from_sql(
                tb,
                config["input"]["jdbc"],
                config["input"]["username"],
                config["input"]["password"],
                output,
                self.config.PARALLEL_JOBS,
            )

            line = sp.stdout.readline()

            while line != "":
                yield self.formatter.format_line(line)

                line = sp.stdout.readline()

                if line == "":
                    line = sp.stderr.readline()