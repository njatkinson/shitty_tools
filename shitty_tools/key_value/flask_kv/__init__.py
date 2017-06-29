from . import client, server

def FlaskKv(app, kv_store, url_prefix =''):
    '''
    :param app: Your flask application 
    :param kv_store: Something that presents a dictionary interface
    :param url_prefix: The sub-path to use for the key value store. Default is /.
    :return: the generated blueprint for the kv store
    '''
    from . import server
    blueprint = server.construct_kv_blueprint(kv_store, url_prefix)
    app.register_blueprint(blueprint)
    return blueprint