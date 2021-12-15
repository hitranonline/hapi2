from . import models

from hapi2.db.sqlalchemy import Updater

class Updater(updaters.Updater):    
    def __init__(self):
        self.models = models
