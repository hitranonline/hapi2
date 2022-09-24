import importlib

from hapi2.config import SETTINGS, VARSPACE

def init():

    opacity = importlib.import_module('hapi2.opacity.models')

    VARSPACE['opacity'] = opacity