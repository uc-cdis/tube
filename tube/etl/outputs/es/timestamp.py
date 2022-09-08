import re
from datetime import datetime
import calendar
from psycopg2.sql import SQL, Identifier
from tube.utils.db import execute_sql_query_return_first_item


def get_backup_version(index_name):
    res = re.match("^[0-9]+", index_name)
    if res is not None:
        return int(res.group()) + 1
    return 0


def to_utc_time(dt):
    utc_time = calendar.timegm(dt.utctimetuple()) + dt.microsecond / 1e6
    return datetime.utcfromtimestamp(utc_time)


def get_latest_utc_transaction_time():
    fields = ["id", "state", "created_datetime"]
    order_by_field = "created_datetime"
    select_clause = SQL("SELECT {fields}").format(
        fields=SQL(",").join(map(Identifier, fields))
    )
    from_clause = SQL("FROM {table}").format(table=Identifier("transaction_logs"))
    where_clause = SQL("WHERE state='SUCCEEDED' ORDER BY created_datetime DESC").format(
        order_by_field=Identifier(order_by_field)
    )
    limit_clause = SQL("LIMIT 1")
    query = SQL(" ").join(
        [select_clause, from_clause, where_clause, limit_clause, SQL(";")]
    )
    return to_utc_time(execute_sql_query_return_first_item(query)[2])


def check_exists_all_indices(es, index_names):
    for index_name in index_names:
        if not es.indices.exists_alias(name=index_name):
            return False
    return True


def get_latest_time_indices_built(es, index_names):
    timestamp = None
    count = 0
    for index_name in index_names:
        versioned_index = list(es.indices.get_alias(name=index_name).keys())[0]
        if not es.indices.exists_alias(index=versioned_index, name="time_*"):
            return None
        new_timestamp = get_timestamp_from_index(es, versioned_index)
        if timestamp is None or timestamp != new_timestamp:
            timestamp = new_timestamp
            count = 1
        else:
            count += 1
    if count == len(index_names):
        return timestamp
    return None


def check_to_run_etl(es, index_names):
    if not check_exists_all_indices(es, index_names):
        return True

    time_from_es = get_latest_time_indices_built(es, index_names)
    latest_transaction_time = get_latest_utc_transaction_time()

    if time_from_es is None or time_from_es < latest_transaction_time:
        return True
    return False


def timestamp_from_transaction_time(dt):
    return "time_{trans_time}".format(trans_time=dt.strftime("%Y-%m-%dT%H-%M-%S"))


def get_timestamp_from_index(es, versioned_index):
    res = es.indices.get_alias(index=versioned_index, name="time_*")
    iso_str = list(res[versioned_index]["aliases"].keys())[0].replace("plus", "+")[5:]
    return datetime.strptime(iso_str, "%Y-%m-%dT%H-%M-%S")


def putting_timestamp(es, index_to_write):
    latest_transaction_time = get_latest_utc_transaction_time()
    es.indices.put_alias(
        index=index_to_write,
        name=timestamp_from_transaction_time(latest_transaction_time),
    )
