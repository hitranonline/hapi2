from ..base import create_engine
from .models import Base, make_session

from hapi2.config import SETTINGS, VARSPACE

def init():
    
    # Create engine.
    VARSPACE['engine'] = create_engine('sqlite:///%s'%\
      (SETTINGS['database']),echo=SETTINGS['echo'])
    
    # Create schema.  
    Base.metadata.create_all(VARSPACE['engine'])

    # Create session.
    VARSPACE['session'] = make_session(VARSPACE['engine'])
    
    print('Database name: %s'%SETTINGS['database'])
    print('Database engine: %s'%SETTINGS['engine'])
