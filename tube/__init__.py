import cdislogging
import flask
import tube.blueprints
from flask.ext.cors import CORS


app = flask.Flask(__name__)
CORS(app=app, headers=['content-type', 'accept'], expose_headers='*')


def app_init(app, settings='tube.settings'):
    app.logger.addHandler(cdislogging.get_stream_handler())
    app.config.from_object(settings)
    print(app.config)


def app_register_blueprints(app):
    app.register_blueprint(tube.blueprints.blueprint, url_prefix='/import')

    @app.route('/')
    def root():
        """
        Register the root URL.
        """
        endpoints = {
            'import endpoint': '/import'
        }
        return flask.jsonify(endpoints)

