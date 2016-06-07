import flask
from pymongo import MongoClient

app = flask.Flask('ptocore')

def get_mongo():
    mongo = getattr(flask.g, '_mongo', None)
    if mongo is None:
        mongo = flask.g._mongo = MongoClient("mongodb://curator:ah8NSAdoITjT49M34VqZL3hEczCHjbcz@localhost/analysis")
    return mongo

def get_analyzers_coll():
    return get_mongo().analysis.analyzers

@app.route('/analyzer', methods=['GET'])
def list_analyzers():
    analyzers = [cursor['_id'] for cursor in get_analyzers_coll().find({}, {'_id':1})]
    return flask.jsonify({'analyzers': analyzers})

@app.route('/analyzer/<analyzer_id>')
def analyzer_state(analyzer_id):
    analyzer = get_analyzers_coll().find_one({'_id': analyzer_id})
    return flask.jsonify(analyzer)

@app.route('/analyzer/<analyzer_id>', methods=['POST'])
def analyzer_create(analyzer_id):
    pass

@app.route('/analyzer/<analyzer_id>/disable', methods=['PUT'])
def disable_analyzer(analyzer_id):
    analyzers_coll = get_analyzers_coll()
    analyzers_coll.update_one({'_id': analyzer_id}, {'$set': {'wish': 'disable'}})

@app.route('/analyzer/<analyzer_id>/enable', methods=['PUT'])
def enable_analyzer(analyzer_id):
    analyzers_coll = get_analyzers_coll()
    analyzers_coll.update_one({'_id': analyzer_id}, {'$set': {'wish': 'enable'}})

@app.route('/analyzer/<analyzer_id>/cancel', methods=['PUT'])
def enable_analyzer(analyzer_id):
    analyzers_coll = get_analyzers_coll()
    analyzers_coll.update_one({'_id': analyzer_id}, {'$set': {'wish': 'cancel'}})



if __name__ == '__main__':
    app.run(debug=True)
