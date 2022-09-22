import importlib

from hapi2.config import SETTINGS, VARSPACE

def init():

    prov_backend = importlib.import_module('hapi2.provenance.types')

    VARSPACE['prov_backend'] = prov_backend