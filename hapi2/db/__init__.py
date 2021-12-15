import importlib

from hapi2.config import SETTINGS, VARSPACE

def init():

    engine = SETTINGS['engine']

    if engine in ['sqlite','mysql']: # core plugins
        backend = importlib.import_module('hapi2.db.sqlalchemy.'+engine)
    else:
        backend = importlib.import_module('hapi2_db_'+engine) # external plugins
    
    backend.init()
    
    VARSPACE['db_backend'] = backend
