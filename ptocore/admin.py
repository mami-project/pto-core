from threading import RLock

import flask
from jsonschema import validate
from pymongo import MongoClient

from .analyzerstate import AnalyzerState
from .repomanager import procure_repository

app = flask.Flask('ptocore')

BASE_GIT_PATH = '/home/elio/analyzers'

analyzer_create_schema = {
  "type": "object",
  "properties": {
    "input_formats":    {"type": "array", "items": {"type": "string"}},
    "input_types":      {"type": "array", "items": {"type": "string"}},
    "output_types":     {"type": "array", "items": {"type": "string"}},
    "command_line":     {"type": "string" },
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

def get_mongo():
    mongo = getattr(flask.g, '_mongo', None)
    if mongo is None:
        mongo = flask.g._mongo = MongoClient("mongodb://curator:ah8NSAdoITjT49M34VqZL3hEczCHjbcz@localhost/analysis")
    return mongo


def get_analyzers_coll():
    return get_mongo().analysis.analyzers


def get_analyzer_state():
    return AnalyzerState('admin', get_analyzers_coll())


@app.route('/analyzer', methods=['GET'])
def list_analyzers():
    analyzers = [cursor['_id'] for cursor in get_analyzers_coll().find({}, {'_id':1})]
    return flask.jsonify({'analyzers': analyzers})


@app.route('/analyzer/<analyzer_id>')
def request_info(analyzer_id):
    analyzer = get_analyzers_coll().find_one({'_id': analyzer_id})
    return flask.jsonify(analyzer)

@app.route('/analyzer/<analyzer_id>', methods=['POST'])
def request_create(analyzer_id):
    with get_lock():
        analyzer_state = get_analyzer_state()

        config = flask.request.get_json()
        validate(config, analyzer_create_schema)

        # clone repository into directory and checkout commit
        repo_url = config['repo_url']
        repo_commit = config['repo_commit']

        # function will raise error if analyzer_id is not suitable (e.g. contains '/' etc..)
        procure_repository(BASE_GIT_PATH, analyzer_id, repo_url, repo_commit)

        analyzer_state.create_analyzer(analyzer_id, config['input_formats'], config['input_types'],
                                       config['output_types'], config['command_line'], '')

        return flask.jsonify({'success': 'created'})

@app.route('/analyzer/<analyzer_id>/setrepo', methods=['POST'])
def request_setrepo(analyzer_id):
    with get_lock():
        config = flask.request.get_json()
        validate(config, analyzer_setrepo_schema)

        analyzer_state = get_analyzer_state()
        if not analyzer_state[analyzer_id]['state'] == 'disabled':
            raise AnalyzerNotDisabled()

        repo_url = config['repo_url']
        repo_commit = config['repo_commit']

        procure_repository(BASE_GIT_PATH, analyzer_id, repo_url, repo_commit)

        return flask.jsonify({'success': 'repo updated'})


@app.route('/analyzer/<analyzer_id>/disable', methods=['PUT'])
def request_disable(analyzer_id):
    with get_lock():
        analyzer_state = get_analyzer_state()

        if not analyzer_state.request_wish(analyzer_id, 'disable'):
            flask.jsonify({'error': 'cannot request deactivation for \'{}\''.format(analyzer_id)})

        if analyzer_state.check_wish(analyzer_state[analyzer_id], 'disable'):
            print("admin: disabled {} upon request".format(analyzer_id))

        return flask.jsonify({'success': 'requested deactivation for \'{}\''.format(analyzer_id)})


@app.route('/analyzer/<analyzer_id>/enable', methods=['PUT'])
def request_enable(analyzer_id):
    with get_lock():
        analyzer_state = get_analyzer_state()

        analyzer = analyzer_state[analyzer_id]

        if analyzer['state'] in ['error', 'disabled']:
            analyzer_state.transition(analyzer_id, analyzer['state'], 'sensing')


@app.route('/analyzer/<analyzer_id>/cancel', methods=['PUT'])
def request_cancel(analyzer_id):
    with get_lock():
        analyzer_state = get_analyzer_state()
        analyzer_state.request_wish(analyzer_id, 'cancel')


