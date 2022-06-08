from ..base import create_engine
from ..legacy import storage2cache

from .models import Base, make_session

from hapi2.config import SETTINGS, VARSPACE

import os

def init():
    
    # Create engine.
    VARSPACE['engine'] = create_engine('sqlite:///%s'%\
      (os.path.join(SETTINGS['database_dir'],SETTINGS['database'])),
      echo=SETTINGS['echo'])
    
    # Create schema.  
    Base.metadata.create_all(VARSPACE['engine'])

    # Create session.
    VARSPACE['session'] = make_session(VARSPACE['engine'])
    
    print('Database name: %s'%SETTINGS['database'])
    print('Database engine: %s'%SETTINGS['engine'])
    print('Database path: %s'%SETTINGS['database_dir'])
