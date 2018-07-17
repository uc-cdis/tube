import flask
from flask import stream_with_context, Response
from tube.importers.sql_to_hdfs import SqlToHDFS
from tube.formatters import HtmlFormatter

blueprint = flask.Blueprint('import', __name__)


@blueprint.route('/', methods=['GET'])
def call_import_from_sql():
    '''
    Import data from sql database to elasticsearch using Sqoop.
    '''
    sql_to_hdfs = SqlToHDFS(flask.current_app.config, HtmlFormatter())
    return Response(
            stream_with_context(sql_to_hdfs.generate_import_all_tables())
        )
