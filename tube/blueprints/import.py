import flask
from flask import stream_with_context, Response
from tube.utils import get_sql_to_hdfs_config
from tube.importers.sql_to_hdfs import get_all_tables, generate_import

blueprint = flask.Blueprint('import', __name__)


@blueprint.route('/', methods=['GET'])
def call_import_from_sql():
    '''
    Import data from sql database to elasticsearch using Sqoop.
    '''
    return Response(
            stream_with_context(generate_import(
                get_all_tables(flask.current_app.config['PYDBC']),
                get_sql_to_hdfs_config(flask.current_app.config)
            ))
        )

