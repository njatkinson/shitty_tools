from flask import Flask, request, abort, Blueprint
import json
import random
import string


def construct_kv_app(kv_store, url_prefix =''):
    app = Flask(__name__)
    app.register_blueprint(construct_kv_blueprint(kv_store, url_prefix))
    return app


def construct_kv_blueprint(kv_store, url_prefix):
    random_string = ''.join(random.sample(string.ascii_letters, 10))
    blueprint = Blueprint('kv_%s' % random_string, __name__, url_prefix=url_prefix)


    @blueprint.route('/', methods=['GET'])
    def get_keys():
        return json.dumps(kv_store.keys())


    @blueprint.route('/<string:key>', methods=['POST','PUT','PATCH'])
    def write(key):
        kv_store[key] = request.data
        return ('', 204)

    @blueprint.route('/<string:key>', methods=['GET'])
    def read(key):
        try:
            value = kv_store[key]
        except KeyError:
            abort(404)
        return value

    @blueprint.route('/<string:key>', methods=['DELETE'])
    def delete(key):
        try:
            del(kv_store[key])
        except KeyError:
            pass
        return ('', 204)

    return blueprint