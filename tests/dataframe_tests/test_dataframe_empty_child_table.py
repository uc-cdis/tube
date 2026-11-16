"""
Regression tests for the src -> name projection in `translate_table_to_dataframe`.

A node table that exported zero rows used to skip the projection entirely, so the frame
kept its *source* column names while the aggregation step addressed the *mapped* names.
Any `aggregated_props` entry whose `name` differs from its `src` then took the whole ETL
down with `AnalysisException: Column '<mapped name>' does not exist`.

The shape that triggers it, and that these tests reproduce: a non-empty parent and a
non-empty parent->child edge, but a child node table with no rows. The edge keeps the
join non-empty, so the `temp_df.rdd.isEmpty()` guard in `aggregate_with_child_tbl` does
not short-circuit.
"""
import os

import pytest

from tests.util import get_spark_session
from tube.etl.indexers.aggregation.nodes.aggregated_node import Reducer
from tube.etl.indexers.base.prop import PropFactory
from tube.utils.dd import get_properties_types
from tube.utils.general import get_node_id_name

DOC_TYPE = "empty_child_table_regression"


def write_empty_table(root_dir, tbl_name):
    """
    Reproduce what sqoop leaves behind for a table with no rows: the directory and the
    part file both exist, and the part file is empty.
    """
    tbl_dir = os.path.join(str(root_dir), tbl_name)
    os.makedirs(tbl_dir)
    open(os.path.join(tbl_dir, "part-m-00000"), "w").close()


def renaming_prop(name, src):
    """A prop whose mapped name differs from its source column, like `raw_file_data_type`."""
    return PropFactory.adding_prop(DOC_TYPE, name, src, [], fn="set", prop_type=(str,))


def first_child_of_root(translator):
    root_name = translator.parser.root.name
    for node in translator.parser.aggregated_nodes:
        if node.name == root_name and node.children:
            return sorted(node.children, key=lambda c: c.name)[0]
    raise AssertionError("the root of this mapping has no aggregated children")


@pytest.mark.schema_midrc
@pytest.mark.parametrize(
    "translator", [("midrc", "imaging_study", "aggregation", [])], indirect=True
)
def test_empty_node_table_is_still_projected_onto_prop_names(translator, tmp_path):
    """
    The narrow contract: whatever the row count, the frame comes back addressed by the
    mapped names, never by the source column names.
    """
    node = first_child_of_root(translator)
    model_props = get_properties_types(translator.parser.model, node.name)
    assert "data_type" in model_props, f"{node.name} has no data_type to rename"

    node.props = [renaming_prop("child_data_type", "data_type")]
    write_empty_table(tmp_path, node.tbl_name)
    translator.hdfs_path = str(tmp_path)

    df = translator.translate_table_to_dataframe(node, props=node.props)

    assert df.rdd.isEmpty()
    assert "child_data_type" in df.schema.names
    assert "data_type" not in df.schema.names
    assert get_node_id_name(node.name) in df.schema.names


@pytest.mark.schema_midrc
@pytest.mark.parametrize(
    "translator", [("midrc", "imaging_study", "aggregation", [])], indirect=True
)
def test_aggregating_an_empty_child_table_does_not_raise(translator, tmp_path):
    """
    End to end over the call that crashed in production: `aggregate_with_child_tbl` with
    a live edge and an empty child table must aggregate to null, not raise.
    """
    parent_name = translator.parser.root.name
    parent_id = get_node_id_name(parent_name)

    child = first_child_of_root(translator)
    child_id = get_node_id_name(child.name)
    child.add_reducer(Reducer(renaming_prop("child_data_format", "data_format"), "set"))

    write_empty_table(tmp_path, child.tbl_name)
    translator.hdfs_path = str(tmp_path)

    session = get_spark_session(translator.sc)
    df = session.createDataFrame([("parent-1",)], [parent_id])
    edge_df = session.createDataFrame([("parent-1", "child-1")], [parent_id, child_id])

    result = translator.aggregate_with_child_tbl(df, parent_name, edge_df, child)

    assert "child_data_format" in result.schema.names
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0][parent_id] == "parent-1"
    # collect_set over a column that is entirely null yields an empty set
    assert not rows[0]["child_data_format"]
