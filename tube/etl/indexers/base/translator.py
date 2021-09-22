import os
import json
from .lambdas import (
    extract_link,
    extract_link_reverse,
    flatten_files_to_lists,
    get_props,
    get_props_empty_values,
    get_number,
)
from tube.utils.spark import save_rdd_of_dataframe, get_all_files, save_rdds
from pyspark.sql.context import SQLContext
from pyspark.sql.types import StructType, StructField, StringType
from tube.utils.general import get_node_id_name
from pyspark.sql.functions import collect_list, col

from .prop import PropFactory
from tube.etl.indexers.base.lambdas import (
    extract_metadata_to_json,
    extract_metadata_to_tuple,
    map_with_dictionary,
)


def json_export_with_no_key(x, doc_type, root_name):
    x[1][get_node_id_name(doc_type)] = x[0]
    if root_name is not None and doc_type != root_name:
        x[1][get_node_id_name(root_name)] = x[0]
    x[1]["node_id"] = x[0]  # redundant field for backward compatibility with arranger
    return json.dumps(x[1])


class Translator(object):
    """
    The main entry point into the index export process for the mutation indices
    """

    def __init__(self, sc, hdfs_path, writer):
        self.sc = sc
        if sc is not None:
            self.sql_context = SQLContext(self.sc)
        self.writer = writer
        self.hdfs_path = hdfs_path
        self.parser = None
        self.current_step = 0
        self.mapping_dictionary = {}
        self.mapping_broadcasted = None

    def update_types(self):
        self.parser.update_prop_types()
        return self.parser.get_es_types()

    def add_some_additional_props(self, keep_props):
        keep_props.append(self.parser.get_key_prop().name)

    def remove_unnecessary_columns(self, df):
        props = list(PropFactory.get_prop_by_doc_name(self.parser.doc_type).values())
        keep_props = [p.name for p in props]
        self.add_some_additional_props(keep_props)
        rm_props = [p for p in df.schema.names if p not in keep_props]
        return df.drop(*rm_props)

    def read_text_files_of_table(self, table_name, fn_frame_zero):
        files = get_all_files(os.path.join(self.hdfs_path, table_name), self.sc)
        if len(files) == 0:
            return fn_frame_zero(), True
        df = None
        if len(files) > 0:
            df = self.sc.textFile(files[0])
        if len(files) > 1:
            for f in files[1:]:
                df = df.union(self.sc.textFile(f))
        return df, False

    def get_frame_zero_rdd(self):
        df = self.sc.parallelize(
            [("__BLANK_ID__", "__BLANK_VALUE__")]
        )  # to create the frame for empty node
        return df.mapValues(lambda x: [])

    def translate_table(self, table_name, get_zero_frame=None, props=None):
        try:
            df, is_empty = self.read_text_files_of_table(
                table_name, self.get_frame_zero_rdd
            )
            if is_empty:
                return df
            df = df.map(extract_metadata_to_tuple)

            if get_zero_frame:
                if df.isEmpty():
                    df = self.sc.parallelize(
                        [("__BLANK_ID__", "__BLANK_VALUE__")]
                    )  # to create the frame for empty node
                return df.mapValues(lambda x: [])
            if props is not None:
                return self.get_props_from_data_row(df, props)
            return df
        except Exception as ex:
            print("HAPPEN WITH NODE: {}".format(table_name))
            print(ex)

    def get_cols_from_node(self, node_name, props, df, key_name=None):
        col_names = [p.src for p in props]
        cols = []
        for p in props:
            if p.src not in df.schema.names:
                continue
            if p.src == "id":
                cols.append(col(get_node_id_name(node_name)).alias(p.name))
            elif (
                p.name in self.mapping_dictionary
                and self.mapping_broadcasted is not None
            ):
                cols.append(
                    map_with_dictionary(self.mapping_broadcasted, p.src)(
                        col(p.src)
                    ).alias(p.name)
                )
            else:
                cols.append(col(p.src).alias(p.name))
        if "id" not in col_names and key_name is None:
            cols.append(get_node_id_name(node_name))
        elif key_name is not None:
            cols.append(key_name)
        return cols

    def translate_table_to_dataframe(
        self, node, get_zero_frame=None, props=None, key_name=None
    ):
        """
        Get data from SQL table to dataframe
        :param node: node object
        :param get_zero_frame: True if we want to have an empty frame value
        :param props: subset of properties to be extracted. None means get all.
        :return:
        """
        node_tbl_name = node.tbl_name
        node_name = node.name
        props = props if props is not None else node.props
        try:
            df, is_empty = self.read_text_files_of_table(
                node_tbl_name, self.get_empty_dataframe_with_name
            )
            if is_empty:
                return df
            df = df.map(lambda x: extract_metadata_to_json(x, node_name))
            if get_zero_frame and (df is None or df.isEmpty()):
                return self.get_empty_dataframe_with_name(node.name, key_name=key_name)
            new_df = self.sql_context.read.json(df)
            df.unpersist()
            if props is not None:
                cols = self.get_cols_from_node(node_name, props, new_df, key_name)
                return new_df.select(*cols)
            return new_df
        except Exception as ex:
            print("HAPPEN WITH NODE: {}".format(node_tbl_name))
            print(ex)

    def get_empty_dataframe_with_name(self, name, key_name=None):
        if name is None and key_name is None:
            schema = StructType([])
        elif key_name is not None:
            schema = StructType([StructField(key_name, StringType(), False)])
        else:
            schema = StructType(
                [StructField(get_node_id_name(name), StringType(), False)]
            )
        return self.sc.parallelize([]).toDF(schema)

    def get_empty_dataframe_with_columns(self, cols):
        schema = (
            StructType([])
            if cols is None or len(cols) == 0
            else StructType([StructField(c, StringType(), False) for c in cols])
        )
        return self.sc.parallelize([]).toDF(schema)

    def translate_edge(self, table_name, reversed=True):
        """
        Return the edge table that has two columns.
        :param table_name:
        :return: [(child_node_id, parent_node_id)] if not reversed other wise [(parent_node_id, child_node_id)]
        """
        df = self.sc.wholeTextFiles(os.path.join(self.hdfs_path, table_name)).flatMap(
            flatten_files_to_lists
        )
        if reversed:
            return df.map(extract_link_reverse)
        return df.map(extract_link)

    def translate_edge_to_dataframe(self, table_name, src, dst):
        src_id_name = get_node_id_name(src)
        dst_id_name = get_node_id_name(dst)
        df = self.translate_edge(table_name, reversed=False)
        df = df.map(lambda x: json.dumps({src_id_name: x[0], dst_id_name: x[1]}))
        if df is None or df.isEmpty():
            return self.get_empty_dataframe_with_columns([src_id_name, dst_id_name])
        new_df = self.sql_context.read.json(df)
        return new_df

    def write(self, df):
        self.writer.write_dataframe(
            df, self.parser.name, self.parser.doc_type, self.parser.types
        )
        self.writer.create_guppy_array_config(self.parser)

    @staticmethod
    def get_props_from_data_row(df, props, to_tuple=False):
        if df.isEmpty():
            return df.mapValues(get_props_empty_values(props))
        # names is dictionary which maps from the name of source fields in datatable to the list of ids
        # of properties in dataframe
        # example: names = {"gender": [1, 2], project_name: [3]}
        names = {}
        # values is a dictionary which defines the mapping values (if exist) for each field.
        # values = {
        #   "gender": {
        #       1: {"male": "M", "female": "F"},
        #       2: {"male": "Male", "female": "Female}
        #   },
        #   "project_name": {3: {}}
        # }
        values = {}
        for p in props:
            n = names.get(p.src, [])
            n.append(p.id)
            names[p.src] = n
            v = values.get(p.src, {})
            v[p.id] = {}
            for m in p.value_mappings:
                v[p.id][m.original] = m.final
            values[p.src] = v

        return df.mapValues(get_props(names, values))

    def get_props_from_df(self, df, props):
        if df.isEmpty():
            return df.mapValues(get_props_empty_values([p.get("dst") for p in props]))
        prop_ids = [(p.get("src").id, p.get("dst").id) for p in props]
        return df.mapValues(lambda x: {dst: x.get(src) for (src, dst) in prop_ids})

    def restore_prop_name(self, df, props):
        return df.mapValues(
            lambda x: {
                props[k].name if isinstance(get_number(k), int) else k: v
                for (k, v) in list(x.items())
            }
        )

    def get_path_from_step(self, step):
        return os.path.join(
            self.hdfs_path, "output", "{}_{}".format(self.parser.doc_type, str(step))
        )

    def save_dataframe_to_hadoop(self, df):
        save_rdd_of_dataframe(df, self.get_path_from_step(self.current_step), self.sc)
        df.unpersist()

    def save_to_hadoop(self, df):
        save_rdds(df, self.get_path_from_step(self.current_step), self.sc)
        df.unpersist()

    def load_from_hadoop(self):
        return self.sc.pickleFile(self.get_path_from_step(self.current_step - 1))

    def load_from_hadoop_to_dateframe(self):
        return self.sql_context.sparkSession.read.parquet(
            self.get_path_from_step(self.current_step - 1)
        )

    def join_two_dataframe(self, df1, df2, how="inner"):
        join_on_props = [p for p in df1.schema.names if p in df2.schema.names]
        if len(join_on_props) == 0:
            return self.get_empty_dataframe_with_columns([])
        return df1.join(df2, on=join_on_props, how=how).drop_duplicates()

    def translate_joining_props(self, translators):
        pass

    def final_transform_rdd_to_df(self, rdd):
        self.update_types()
        rdd = self.restore_prop_name(rdd, PropFactory.list_props)
        doc_type = self.parser.doc_type
        root_name = self.parser.root
        rdd = rdd.map(lambda x: json_export_with_no_key(x, doc_type, root_name))
        new_df = self.sql_context.read.json(rdd)
        rdd.unpersist()
        return new_df

    def translate_final(self):
        rdd = self.load_from_hadoop()
        return self.final_transform_rdd_to_df(rdd)

    def get_all_value_mapping_dict(self):
        m_dict = {}
        for p in PropFactory.get_prop_by_doc_name(self.parser.doc_type).values():
            for v in p.value_mappings:
                if p.name not in m_dict:
                    m_dict[p.name] = {}
                m_dict[p.name][v.original] = v.final
        return m_dict

    def translate(self):
        pass
