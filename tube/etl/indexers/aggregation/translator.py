import collections
from tube.etl.indexers.base.lambdas import (
    merge_and_fill_empty_props,
    merge_dictionary,
    swap_key_value,
)
from tube.etl.indexers.base.translator import Translator as BaseTranslator
from tube.etl.indexers.aggregation.lambdas import (
    intermediate_frame,
    merge_aggregate_with_reducer,
    seq_aggregate_with_reducer,
    get_frame_zero,
    get_normal_frame,
    get_single_frame_zero_by_func,
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
from ..base.lambdas import sort_by_field, swap_property_as_key, make_key_from_property
from .lambdas import sliding


class Translator(BaseTranslator):
    def __init__(self, sc, hdfs_path, writer, mapping, model, dictionary):
        super(Translator, self).__init__(sc, hdfs_path, writer)
        self.parser = Parser(mapping, model, dictionary)

    def aggregate_intermediate_data_frame(self, child_df, edge_df):
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

    def aggregate_with_count_on_edge_tbl(self, df, edge_df, child):
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

    def aggregate_with_child_tbl(self, df, swapped_df, child):
        child_df = self.translate_table(
            child.tbl_name,
            props=[
                rd.prop
                for rd in child.reducers
                if not rd.done and rd.prop.src is not None
            ],
        ).mapValues(get_normal_frame(child.reducers))

        frame_zero = get_frame_zero(child.reducers)
        temp_df = (
            swapped_df.leftOuterJoin(child_df)
            .map(lambda x: (x[1][0], x[1][1]))
            .mapValues(lambda x: x if x is not None else frame_zero)
        )
        temp_df = temp_df.aggregateByKey(
            frame_zero, seq_aggregate_with_reducer, merge_aggregate_with_reducer
        )
        return df.leftOuterJoin(temp_df).mapValues(lambda x: x[0] + x[1])

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
        return aggregated_dfs[self.parser.root.name].mapValues(
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

    def join_and_aggregate(self, df, joining_df, dual_props, joining_node):
        frame_zero = get_frame_zero(joining_node.getting_fields)
        joining_df = (
            self.get_props_from_df(joining_df, dual_props)
            .mapValues(get_normal_frame(joining_node.getting_fields))
            .aggregateByKey(
                frame_zero, seq_aggregate_with_reducer, merge_aggregate_with_reducer
            )
            .mapValues(lambda x: {x1: x2 for (x0, x1, x2) in x})
        )
        df = df.leftOuterJoin(joining_df).mapValues(
            lambda x: merge_and_fill_empty_props(x, [p.get("dst") for p in dual_props])
        )
        joining_df.unpersist()
        return df

    def join_no_aggregate(self, df, joining_df, dual_props):
        joining_df = self.get_props_from_df(joining_df, dual_props)
        df = df.leftOuterJoin(joining_df).mapValues(
            lambda x: merge_and_fill_empty_props(x, [p.get("dst") for p in dual_props])
        )
        joining_df.unpersist()
        return df

    def join_to_an_index(self, df, translator, joining_node):
        """
        Perform the join between indices. It will:
         - load rdd to be join from HDFS
         - Joining with df
        :param df: rdd of translator that does the join
        :param translator: translator has rdd to be join this translator
        :param joining_node: joining_node define in yaml file.
        :return:
        """
        joining_df = translator.load_from_hadoop()

        # For joining two indices, we need to swap the property field and key of one of the index.
        # based on join_on value in the etlMapping, we know what field is used as joining field.
        # We swap the index that have name of key field different than the name of joining field
        joining_df_key_id = translator.parser.get_key_prop().id
        id_field_in_joining_df = translator.parser.get_prop_by_name(
            joining_node.joining_field
        ).id
        # field which is identity of a node is named as _{node}_id now
        # before in etl-mapping for joining_props, we use {node}_id
        # for backward compatibility, we check first with the value in mapping file.
        # if there is not any Prop object like that, we check with new format _{node}_id
        id_field_in_df = self.parser.get_prop_by_name(joining_node.joining_field)
        if id_field_in_df is None:
            id_field_in_df = self.parser.get_prop_by_name(
                get_node_id_name(self.parser.doc_type)
            )
        if id_field_in_df is None:
            raise Exception(
                "{} field does not exist in index {}".format(
                    joining_node.joining_field, self.parser.doc_type
                )
            )
        id_field_in_df_id = id_field_in_df.id
        df_key_id = self.parser.get_key_prop().id

        swap_df = False
        if joining_df_key_id != id_field_in_joining_df:
            joining_df = swap_property_as_key(
                joining_df, id_field_in_joining_df, joining_df_key_id
            )
        if df_key_id != id_field_in_df_id:
            df = swap_property_as_key(df, id_field_in_df_id, df_key_id)
            swap_df = True

        # Join can be done with or without an aggregation function like max, min, sum, ...
        # these two type of join requires different map-reduce steos
        props_with_fn, props_without_fn = self.get_joining_props(
            translator, joining_node
        )
        if len(props_with_fn) > 0:
            df = self.join_and_aggregate(df, joining_df, props_with_fn, joining_node)
        if len(props_without_fn) > 0:
            df = self.join_no_aggregate(df, joining_df, props_without_fn)

        if swap_df:
            df = swap_property_as_key(df, df_key_id, id_field_in_df_id)
        return df

    def ensure_project_id_exist(self, df):
        project_id_prop = self.parser.get_prop_by_name(PROJECT_ID)
        if project_id_prop is None:
            project_id_prop = PropFactory.adding_prop(
                self.parser.doc_type, PROJECT_ID, None, [], prop_type=(str,)
            )
            project_code_id = self.parser.get_prop_by_name(PROJECT_CODE).id
            program_name_id = self.parser.get_prop_by_name(PROGRAM_NAME).id
            df = df.mapValues(
                lambda x: merge_dictionary(
                    x,
                    {
                        project_id_prop.id: "{}-{}".format(
                            x.get(program_name_id), x.get(project_code_id)
                        )
                    },
                )
            )
        return df

    def translate(self):
        root_tbl = self.parser.root.tbl_name
        root_df = self.translate_table(root_tbl, props=self.parser.props)
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

    def translate_parent(self, root_df):
        if len(self.parser.parent_nodes) == 0:
            return root_df
        root_id = self.parser.get_key_prop().id
        for f in self.parser.parent_nodes:
            df = self.translate_table(self.parser.root.tbl_name, props=[])
            n = f.head
            first = True
            while n is not None:
                edge_tbl = n.edge_up_tbl
                df = df.join(self.translate_edge(edge_tbl, reversed=False))
                if first:
                    df = df.map(
                        lambda x: (x[1][1], ({root_id: x[0]},) + (x[1][0],))
                    ).mapValues(lambda x: merge_dictionary(x[0], x[1]))
                    first = False
                else:
                    df = df.map(lambda x: (x[1][1], x[1][0]))
                cur_props = n.props
                tbl = n.tbl
                n_df = self.translate_table(tbl, props=cur_props)

                df = n_df.join(df).mapValues(
                    lambda x: merge_and_fill_empty_props(x, cur_props)
                )
                n = n.child
            df = df.map(lambda x: make_key_from_property(x[1], root_id))
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
        root_id = self.parser.get_key_prop().id
        for f in self.parser.special_nodes:
            if f.fn[0] == "sliding":
                df = self.translate_table(self.parser.root.tbl_name, props=[])
                n = f.head
                first = True
                while n is not None:
                    edge_tbl = n.edge_up_tbl
                    df = df.join(self.translate_edge(edge_tbl))
                    if first:
                        df = df.map(
                            lambda x: (x[1][1], ({root_id: x[0]},) + (x[1][0],))
                        ).mapValues(lambda x: merge_dictionary(x[0], x[1]))
                        first = False
                    else:
                        df = df.map(lambda x: (x[1][1], x[1][0]))
                    cur_props = n.props
                    tbl = n.tbl
                    n_df = self.translate_table(tbl, props=cur_props)
                    df = n_df.join(df).mapValues(
                        lambda x: merge_and_fill_empty_props(x, cur_props)
                    )
                    n = n.child
                df = df.map(lambda x: make_key_from_property(x[1], root_id))
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
