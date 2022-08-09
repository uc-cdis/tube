import collections
from tube.etl.indexers.base.lambdas import (
    merge_and_fill_empty_props,
    merge_two_dicts_with_subset_props_from_left,
    merge_dictionary,
    swap_key_value,
    sort_by_field,
    swap_property_as_key,
    make_key_from_property,
    merge_aggregate_with_reducer,
    seq_aggregate_with_reducer,
    flatmap_nested_list_rdd,
    from_program_name_project_code_to_project_id,
)
from tube.etl.indexers.base.translator import Translator as BaseTranslator
from tube.etl.indexers.aggregation.lambdas import (
    intermediate_frame,
    get_frame_zero,
    get_normal_frame,
    get_single_frame_zero_by_func,
    get_dict_zero_with_set_fn,
    seq_or_merge_aggregate_dictionary_with_set_fn,
    get_normal_dict_item,
)
from tube.etl.indexers.base.prop import PropFactory
from tube.utils.dd import get_node_table_name
from tube.utils.general import (
    get_node_id_name,
    get_node_id_name_without_prefix,
    PROJECT_ID,
    PROJECT_CODE,
    PROGRAM_NAME,
)
from .parser import Parser
from .nested.translator import Translator as NestedTranslator
from .lambdas import sliding
from tube.etl.indexers.base.logic import execute_filter
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

COMPOSITE_JOINING_FIELD = "_joining_keys_"


def create_composite_field_from_joining_fields(df, id_fields, key_id):
    if len(id_fields) > 1:
        df = df.filter(
            lambda x: len([x[1].get(f_id) is None for f_id in id_fields])
            < len(id_fields)
        )
        return df.map(
            lambda x: (
                x[0],
                merge_dictionary(
                    x[1],
                    {
                        COMPOSITE_JOINING_FIELD: tuple(
                            [
                                x[1].get(f_id) if f_id != key_id else x[0]
                                for f_id in id_fields
                            ]
                        )
                    },
                ),
            )
        )
    f_id = id_fields[0]
    df = df.filter(lambda x: x[1].get(f_id) is not None)
    return df.map(
        lambda x: (
            x[0],
            merge_dictionary(
                x[1],
                {COMPOSITE_JOINING_FIELD: x[1].get(f_id) if f_id != key_id else x[0]},
            ),
        )
    )


def get_normal_frame_schema():
    return StructType([
        StructField("guid", StringType(), False),
        StructField("agg", ArrayType(
            StructType([
                StructField("fn", StringType(), False),
                StructField("field_id", IntegerType(), False),
                StructField("arr", ArrayType(StringType()))
            ])
        ))
    ])


def get_edge_schema():
    return StructType([
        StructField("guid", StringType(), False),
        StructField("a_guid", StringType(), False)
    ])


class Translator(BaseTranslator):
    def __init__(self, sc, hdfs_path, writer, mapping, model, dictionary):
        super(Translator, self).__init__(sc, hdfs_path, writer)
        self.parser = Parser(mapping, model, dictionary)
        nest_props = mapping.get("nested_props")
        self.nested_translator = (
            NestedTranslator(
                sc,
                hdfs_path,
                writer,
                {
                    "root": mapping.get("root"),
                    "doc_type": mapping.get("doc_type"),
                    "name": mapping.get("name"),
                    "nested_props": mapping.get("nested_props"),
                },
                model,
                dictionary,
            )
            if nest_props is not None
            else None
        )

    def update_types(self):
        es_mapping = super(Translator, self).update_types()
        properties = es_mapping.get(self.parser.doc_type).get("properties")
        if self.nested_translator is not None:
            nested_types = self.nested_translator.update_types()
            for a in self.nested_translator.parser.array_types:
                if a not in self.parser.array_types:
                    self.parser.array_types.append(a)
            properties.update(nested_types[self.parser.root]["properties"])
        return es_mapping

    @staticmethod
    def aggregate_intermediate_data_frame(child_df, edge_df):
        """
        Perform aggregation in the intermediate steps (attached to a AggregationNode - child node)
        :param child_df: rdd of the child table which will be aggregated
        :param edge_df: rdd of the edge connected from the parent to child node.
        :return:
        """
        frame_zero = tuple(
            [get_single_frame_zero_by_func(i[0], i[1]) for i in child_df.first()[1]]
        )
        temp_df = (
            edge_df.leftOuterJoin(child_df)
            .map(lambda x: (x[1][0], x[1][1]))
            .mapValues(lambda x: x if x is not None else frame_zero)
        )
        return temp_df.aggregateByKey(
            frame_zero, seq_aggregate_with_reducer, merge_aggregate_with_reducer
        )

    @staticmethod
    def aggregate_with_count_on_edge_tbl(df, edge_df, child):
        """
        Do the aggregation which only based on the edge table (count, sum, ...)
        :param df:
        :param edge_df:
        :param child:
        :return:
        """
        count_reducer = None
        for reducer in child.reducers:
            if reducer.prop.src is None and reducer.fn == "count":
                count_reducer = reducer
                break

        if count_reducer is None:
            # if there is no reducer, group by parent key and get out empty value
            count_df = edge_df.groupByKey().mapValues(lambda x: ())
        else:
            # if there is no reducer, group by parent key and get out the number of children
            # only non-leaf nodes goes through this step
            count_df = (
                edge_df.groupByKey()
                .mapValues(lambda x: len([i for i in x if i is not None]))
                .mapValues(intermediate_frame(count_reducer.prop))
            )
            count_reducer.done = True
        # combine value lists new counted dataframe to existing one
        return (
            count_df
            if df is None
            else df.leftOuterJoin(count_df).mapValues(lambda x: x[0] + x[1])
        )

    def aggregate_with_child_tbl(self, rdd, swapped_rdd, child):
        child_rdd = self.translate_table(
            child.tbl_name,
            props=[
                rd.prop
                for rd in child.reducers
                if not rd.done and rd.prop.src is not None
            ],
        ).mapValues(get_normal_frame(child.reducers))
        child_schema = get_normal_frame_schema()
        child_df = self.sql_context.createDataFrame(child_rdd, schema=child_schema)
        edge_schema = get_edge_schema()
        swapped_df = self.sql_context.createDataFrame(swapped_rdd, schema=edge_schema)
        temp_df = self.join_two_dataframe(swapped_df, child_df, "left_outer")
        temp_rdd = temp_df.rdd

        frame_zero = get_frame_zero(child.reducers)
        temp_rdd = (
            temp_rdd.map(lambda x: (x[1], x[2]))
            .mapValues(lambda x: x if x is not None else frame_zero)
        )
        temp_rdd = temp_rdd.aggregateByKey(
            frame_zero, seq_aggregate_with_reducer, merge_aggregate_with_reducer
        )
        return rdd.leftOuterJoin(temp_rdd).mapValues(lambda x: x[0] + x[1])

    def aggregate_nested_properties(self):
        """
        Create aggregated nodes from the deepest level of the aggregation tree.
        A map/reduce step will be performed for an aggregated node when the map/reduce step in all its children nodes
        were done.
        :return:
            A dataframe including all the aggregated fields
        """
        aggregated_dfs = {}
        for n in self.parser.aggregated_nodes:
            df = None
            key_df = self.translate_table(n.tbl_name, get_zero_frame=True)
            for child in n.children:
                if child.no_children_to_map == 0:
                    # Read all associations from edge table that link between parent and child one
                    edge_df = key_df.leftOuterJoin(
                        self.translate_edge(child.edge_up_tbl)
                    ).mapValues(lambda x: x[1])
                    df = self.aggregate_with_count_on_edge_tbl(df, edge_df, child)
                    no_of_remaining_reducers = len(
                        [r for r in child.reducers if not r.done]
                    )
                    if no_of_remaining_reducers > 0:
                        df = self.aggregate_with_child_tbl(
                            df, swap_key_value(edge_df), child
                        )

                    # aggregate values for child node (it is a sum for non-leaf node that has been in the hash,
                    # and a count leaf node)
                    df = (
                        df
                        if child.__key__() not in aggregated_dfs
                        else df.leftOuterJoin(
                            self.aggregate_intermediate_data_frame(
                                aggregated_dfs[child.__key__()], swap_key_value(edge_df)
                            )
                        ).mapValues(lambda x: x[0] + x[1])
                    )
                    n.no_children_to_map -= 1
                    edge_df.unpersist()
                else:
                    df = key_df
            aggregated_dfs[n.__key__()] = df
            key_df.unpersist()
        return aggregated_dfs[self.parser.root].mapValues(
            lambda x: {x1: x2 for (x0, x1, x2) in x}
        )

    def get_direct_children(self, root_df):
        """
        Get data of all directed nodes and attach to root node
        :param root_df:
        :return:
        """
        for n in self.parser.flatten_props:
            # if n is a child of root node, we don't need to swap order of the pair ids
            edge_df = self.translate_edge(n.edge, not n.props_from_child)
            props = n.props
            if n.sorted_by is not None:
                sorting_prop = PropFactory.adding_prop(
                    self.parser.doc_type, n.sorted_by, n.sorted_by, []
                )
                props.append(sorting_prop)
            child_df = self.translate_table(n.tbl_name, props=props)
            child_by_root = edge_df.join(child_df).map(
                lambda x: tuple([x[1][0], x[1][1]])
            )
            if n.sorted_by is not None:
                child_by_root = child_by_root.groupByKey()
                child_by_root = child_by_root.mapValues(
                    lambda it: sort_by_field(it, sorting_prop.id, n.desc_order)[0]
                )
                child_by_root = child_by_root.mapValues(
                    lambda x: {
                        k: v for (k, v) in list(x.items()) if k != sorting_prop.id
                    }
                )
            root_df = root_df.leftOuterJoin(child_by_root).mapValues(
                lambda x: merge_and_fill_empty_props(x, n.props)
            )
            child_df.unpersist()
            child_by_root.unpersist()
        return root_df

    def get_joining_props(self, translator, joining_index):
        """
        Get joining props added by an additional join between indices/documents
        :param joining_index: Joining index created from parser
        :return:
        """
        props_with_fn = []
        props_without_fn = []
        for r in joining_index.getting_fields:
            src_prop = translator.parser.get_prop_by_name(r.prop.name)
            # field which is identity of a node is named as _{node}_id now
            # before in etl-mapping for joining_props, we use {node}_id
            # for backward compatibility, we check first with the value in mapping file.
            # if there is not any Prop object like that, we check with new format _{node}_id
            if src_prop is None and r.prop.src == get_node_id_name_without_prefix(
                translator.parser.doc_type
            ):
                src_prop = translator.parser.get_prop_by_name(
                    get_node_id_name(translator.parser.doc_type)
                )
            dst_prop = self.parser.get_prop_by_name(r.prop.name)
            if r.fn is None:
                props_without_fn.append({"src": src_prop, "dst": dst_prop})
            else:
                props_with_fn.append({"src": src_prop, "dst": dst_prop})
        return props_with_fn, props_without_fn

    def join_and_aggregate(self, df, joining_df, left_props, dual_props, joining_node):
        frame_zero = get_frame_zero(joining_node.getting_fields)
        joining_df = (
            self.get_props_from_df(joining_df, dual_props)
            .mapValues(get_normal_frame(joining_node.getting_fields))
            .aggregateByKey(
                frame_zero, seq_aggregate_with_reducer, merge_aggregate_with_reducer
            )
            .mapValues(lambda x: flatmap_nested_list_rdd(x))
            .mapValues(lambda x: {x1: x2 for (x0, x1, x2) in x})
        )
        df = df.join(joining_df).mapValues(
            lambda x: merge_two_dicts_with_subset_props_from_left(
                x, left_props, [p.get("dst") for p in dual_props]
            )
        )
        joining_df.unpersist()
        return df

    def join_no_aggregate(self, df, joining_df, left_props, dual_props):
        joining_df = self.get_props_from_df(joining_df, dual_props)
        df = df.join(joining_df).mapValues(
            lambda x: merge_two_dicts_with_subset_props_from_left(
                x, left_props, [p.get("dst") for p in dual_props]
            )
        )
        joining_df.unpersist()
        return df

    def join_to_an_index(self, original_df, translator, joining_node):
        """
        Perform the join between indices. It will:
        - load rdd to be join from HDFS
        - Joining with df
        :param original_df: rdd of translator that does the join
        :param translator: translator has rdd to be join this translator
        :param joining_node: joining_node define in yaml file.
        :return:
        """
        joining_df = translator.load_from_hadoop()
        changing_df = self.load_from_hadoop()

        # For joining two indices, we need to swap the property field and key of one of the index.
        # based on join_on value in the etlMapping, we know what field is used as joining field.
        # We swap the index that have name of key field different than the name of joining field
        joining_df_key_id = translator.parser.get_key_prop().id
        id_fields_in_joining_df = [
            translator.parser.get_prop_by_name(f).id
            for f in joining_node.joining_fields
        ]
        # field which is identity of a node is named as _{node}_id now
        # before in etl-mapping for joining_props, we use {node}_id
        # for backward compatibility, we check first with the value in mapping file.
        # if there is not any Prop object like that, we check with new format _{node}_id
        id_fields_in_df = [
            self.parser.get_prop_by_name(f) for f in joining_node.joining_fields
        ]
        if id_fields_in_df is None:
            id_fields_in_df = self.parser.get_prop_by_name(
                get_node_id_name(self.parser.doc_type)
            )
        if id_fields_in_df is None:
            raise Exception(
                "one of {} fields do not exist in index {}".format(
                    joining_node.joining_fields, self.parser.doc_type
                )
            )
        id_fields_in_df_id = [f.id for f in id_fields_in_df]
        df_key_id = self.parser.get_key_prop().id

        swap_df = False
        if (
            len(id_fields_in_joining_df) > 1
            or joining_df_key_id not in id_fields_in_joining_df
        ):
            joining_df = create_composite_field_from_joining_fields(
                joining_df, id_fields_in_joining_df, joining_df_key_id
            )
            joining_df = swap_property_as_key(
                joining_df, COMPOSITE_JOINING_FIELD, joining_df_key_id
            )
        if len(id_fields_in_df_id) > 1 or df_key_id not in id_fields_in_df_id:
            changing_df = create_composite_field_from_joining_fields(
                changing_df, id_fields_in_df_id, df_key_id
            )
            changing_df = swap_property_as_key(
                changing_df, COMPOSITE_JOINING_FIELD, df_key_id
            )
            swap_df = True

        # Join can be done with or without an aggregation function like max, min, sum, ...
        # these two type of join requires different map-reduce steos
        props_with_fn, props_without_fn = self.get_joining_props(
            translator, joining_node
        )
        left_props = [df_key_id] if swap_df else []
        if len(props_with_fn) > 0:
            changing_df = self.join_and_aggregate(
                changing_df, joining_df, left_props, props_with_fn, joining_node
            )
        if len(props_without_fn) > 0:
            changing_df = self.join_no_aggregate(
                changing_df, joining_df, left_props, props_without_fn
            )

        if swap_df:
            changing_df = swap_property_as_key(changing_df, df_key_id)

        all_props = props_with_fn + props_without_fn
        changing_df = original_df.leftOuterJoin(changing_df).mapValues(
            lambda x: merge_and_fill_empty_props(x, [p.get("dst") for p in all_props])
        )

        return changing_df

    def ensure_project_id_exist(self, df):
        project_id_prop = self.parser.get_prop_by_name(PROJECT_ID)
        if project_id_prop is None:
            project_id_prop = PropFactory.adding_prop(
                self.parser.doc_type, PROJECT_ID, None, [], prop_type=(str,)
            )
            project_code_id = self.parser.get_prop_by_name(PROJECT_CODE).id
            program_name_id = self.parser.get_prop_by_name(PROGRAM_NAME).id
            project_id_id = project_id_prop.id
            df = df.mapValues(
                lambda x: from_program_name_project_code_to_project_id(
                    x, project_id_id, program_name_id, project_code_id
                )
            )
        return df

    def translate(self):
        root_df = self.translate_table(self.parser.root.tbl_name, props=self.parser.props)
        root_df = self.translate_special(root_df)
        root_df = self.translate_parent(root_df)
        root_df = self.get_direct_children(root_df)
        root_df = self.ensure_project_id_exist(root_df)
        if len(self.parser.aggregated_nodes) == 0:
            return root_df
        return root_df.join(self.aggregate_nested_properties()).mapValues(
            lambda x: merge_dictionary(x[0], x[1])
        )

    def translate_joining_props(self, translators):
        """
        Perform the join between the index/document created by this translator with
        the indices/documents created by translators in the paramter
        :param translators: translators containing indices that need to be joined
        :return:
        """
        df = self.load_from_hadoop()
        for j in self.parser.joining_nodes:
            df = self.join_to_an_index(df, translators[j.joining_index], j)
        return df

    def walk_through_graph(self, root_tbl, root_id, f, is_reversal=True):
        df = self.translate_table(root_tbl, props=[])
        n = f.head
        first = True
        list_props = []
        while n is not None:
            edge_tbl = n.edge_up_tbl
            df = df.join(self.translate_edge(edge_tbl, reversed=is_reversal))
            if first:
                df = df.map(
                    lambda x: (x[1][1], ({root_id: x[0]},) + (x[1][0],))
                ).mapValues(lambda x: merge_dictionary(x[0], x[1]))
                first = False
            else:
                df = df.map(lambda x: (x[1][1], x[1][0]))
            cur_props = n.props
            list_props.extend(cur_props)
            tbl_name = n.tbl_name
            n_df = self.translate_table(tbl_name, props=cur_props)
            df = n_df.join(df).mapValues(
                lambda x: merge_and_fill_empty_props(x, cur_props)
            )
            n = n.child
        dict_zero = get_dict_zero_with_set_fn(list_props)
        df = (
            df.map(lambda x: make_key_from_property(x[1], root_id))
            .mapValues(get_normal_dict_item())
            .aggregateByKey(
                dict_zero,
                seq_or_merge_aggregate_dictionary_with_set_fn,
                seq_or_merge_aggregate_dictionary_with_set_fn,
            )
        )
        return df

    def translate_parent(self, root_df):
        if len(self.parser.parent_nodes) == 0:
            return root_df
        root_tbl = get_node_table_name(self.parser.model, self.parser.root)
        root_id = self.parser.get_key_prop().id
        for f in self.parser.parent_nodes:
            df = self.walk_through_graph(root_tbl, root_id, f, is_reversal=False)
            root_df = root_df.leftOuterJoin(df).mapValues(
                lambda x: merge_dictionary(x[0], x[1])
            )
        return root_df

    def translate_special(self, root_df):
        """
        If etlMapping have special_props entry that defines a special function, run this translation
        :param root_df: The special function also have the same root with hosted document (case or subject)
        :return: Return the origin rdd with result from special function included inside
        """
        if len(self.parser.special_nodes) == 0:
            return root_df
        root_tbl = get_node_table_name(self.parser.model, self.parser.root)
        root_id = self.parser.get_key_prop().id
        for f in self.parser.special_nodes:
            if f.fn[0] == "sliding":
                df = self.walk_through_graph(root_tbl, root_id, f)
                (n, fn1, fn2) = tuple(f.fn[1:])
                fid = self.parser.get_prop_by_name(f.name).id
                df = df.mapValues(
                    lambda x: tuple(
                        [
                            v
                            for (k, v) in list(
                                collections.OrderedDict(sorted(x.items())).items()
                            )
                        ]
                    )
                )
                df = sliding(df, int(n.strip()), fn1.strip(), fn2.strip()).mapValues(
                    lambda x: {fid: x}
                )
                root_df = root_df.leftOuterJoin(df).mapValues(
                    lambda x: merge_dictionary(x[0], x[1])
                )
        return root_df

    def translate_final(self):
        nested_df = (
            self.nested_translator.translate()
            if self.nested_translator is not None
            else None
        )
        df = super(Translator, self).translate_final()
        if self.nested_translator is not None:
            df = self.join_two_dataframe(df, nested_df, how="left_outer")
        return execute_filter(df, self.parser.filter) if self.parser.filter else df
