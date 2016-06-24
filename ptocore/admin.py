from threading import RLock
import os

import flask
from flask_cors import CORS
from jsonschema import validate
from pymongo import MongoClient

from .analyzerstate import AnalyzerState
from .repomanager import procure_repository
from .coreconfig import CoreConfig

app = flask.Flask('ptocore')
CORS(app)

analyzer_create_schema = {
  "type": "object",
  "properties": {
    "input_formats":    {"type": "array", "items": {"type": "string"}},
    "input_types":      {"type": "array", "items": {"type": "string"}},
    "output_types":     {"type": "array", "items": {"type": "string"}},
    "command_line":     {"type": "array", "items": {"type": "string"}},
    "repo_url":         {"type": "string" },
    "repo_commit":      {"type": "string" },
  },
  "required": ["input_formats", "input_types", "output_types", "command_line", "repo_url", "repo_commit"]
}

analyzer_setrepo_schema = {
    "type": "object",
    "properties": {
        "repo_url":     {"type": "string"},
        "repo_commit":  {"type": "string"}
    },
    "required": ["repo_url", "repo_commit"]
}


class AnalyzerNotDisabled(Exception):
    pass


def get_lock():
    lock = getattr(flask.g, '_lock', None)
    if lock is None:
        lock = flask.g._lock = RLock()
    return lock


def get_core_config():
    core_config = getattr(flask.g, '_core_config', None)
    if core_config is None:
        config_file = os.environ['PTO_CONFIG_FILE']
        with open(config_file, 'rt') as fp:
            core_config = flask.g._core_config = CoreConfig('admin', fp)
    return core_config


def get_analyzer_state():
    cc = get_core_config()
    return AnalyzerState('admin', cc.analyzers_coll)


@app.route('/static/<path:path>')
def send_static(path):
    cc = get_core_config()
    return flask.send_from_directory(cc.admin_static_path, path)


@app.route('/analyzer', methods=['GET'])
def list_analyzers():
    cc = get_core_config()
    cursor = cc.analyzers_coll.find({}, {'_id': 1, 'state': 1})

    records = [{'analyzer_id': doc['_id'], 'state': doc['state']} for doc in cursor]
    return flask.jsonify(records)


@app.route('/analyzer/<analyzer_id>')
def request_info(analyzer_id):
    cc = get_core_config()
    analyzer = cc.analyzers_coll.find_one({'_id': analyzer_id})
    return flask.jsonify(analyzer)


@app.route('/analyzer/<analyzer_id>/create', methods=['POST'])
def request_create(analyzer_id):
    with get_lock():
        cc = get_core_config()
        analyzer_state = get_analyzer_state()

        config = flask.request.get_json()
        validate(config, analyzer_create_schema)

        # clone repository into directory and checkout commit
        repo_url = config['repo_url']
        repo_commit = config['repo_commit']


        # function will raise error if analyzer_id is not suitable (e.g. contains '/' etc..)
        procure_repository(cc.admin_base_repo_path, analyzer_id, repo_url, repo_commit)

        repo_path = os.path.join(cc.admin_base_repo_path, analyzer_id)

        analyzer_state.create_analyzer(analyzer_id, config['input_formats'], config['input_types'],
                                   config['output_types'], config['command_line'], repo_path)

        return flask.jsonify({'success': 'created'})


@app.route('/analyzer/<analyzer_id>/setrepo', methods=['POST'])
def request_setrepo(analyzer_id):
    with get_lock():
        cc = get_core_config()
        config = flask.request.get_json()
        validate(config, analyzer_setrepo_schema)

        analyzer_state = get_analyzer_state()
        if not analyzer_state[analyzer_id]['state'] == 'disabled':
            raise AnalyzerNotDisabled()

        repo_url = config['repo_url']
        repo_commit = config['repo_commit']

        procure_repository(cc.admin_base_repo_path, analyzer_id, repo_url, repo_commit)

        return flask.jsonify({'success': 'repo updated'})


@app.route('/analyzer/<analyzer_id>/disable', methods=['PUT'])
def request_disable(analyzer_id):
    with get_lock():
        analyzer_state = get_analyzer_state()

        if not analyzer_state.request_wish(analyzer_id, 'disable'):
            flask.jsonify({'error': 'cannot request deactivation for \'{}\''.format(analyzer_id)})

        if analyzer_state.check_wish(analyzer_state[analyzer_id], 'disable'):
            print("admin: disabled {} upon request".format(analyzer_id))

        return flask.jsonify({'success': 'requested disable for \'{}\''.format(analyzer_id)})


@app.route('/analyzer/<analyzer_id>/enable', methods=['PUT'])
def request_enable(analyzer_id):
    with get_lock():
        analyzer_state = get_analyzer_state()
        print(analyzer_state, analyzer_id)
        analyzer = analyzer_state[analyzer_id]

        print(analyzer)

        if analyzer['state'] in ['error', 'disabled']:
            analyzer_state.transition(analyzer_id, analyzer['state'], 'sensing')
            return flask.jsonify({'success': 'enabled \'{}\''.format(analyzer_id)})
        else:
            return flask.jsonify({'success': 'analyzer \'{}\ is in state \'{}\''.format(analyzer_id, analyzer['state'])})

@app.route('/analyzer/<analyzer_id>/cancel', methods=['PUT'])
def request_cancel(analyzer_id):
    with get_lock():
        analyzer_state = get_analyzer_state()
        analyzer_state.request_wish(analyzer_id, 'cancel')

        if analyzer_state.check_wish(analyzer_state[analyzer_id], 'cancel'):
            print("admin: cancelled {} upon request".format(analyzer_id))

        return flask.jsonify({'success': 'requested cancel for \'{}\''.format(analyzer_id)})
