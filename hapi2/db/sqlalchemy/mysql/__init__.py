from ..base import create_engine
from ..legacy import storage2cache

from .models import Base, make_session

from hapi2.config import SETTINGS, VARSPACE

from getpass import getpass

def init():
    
    # Get password.
    password = SETTINGS['pass']
    if not password:
        password = getpass()
    
    # Create engine.
    VARSPACE['engine'] = create_engine('mysql://%s:%s@localhost/%s?charset=utf8mb4'%\
      (\
       SETTINGS['user'],
       password,
       #SETTINGS['pass'],
       SETTINGS['database'],
       ),
      echo=SETTINGS['echo'])
    
    # Create schema.  
    Base.metadata.create_all(VARSPACE['engine'])

    # Create session.
    VARSPACE['session'] = make_session(VARSPACE['engine'])
    
    print('Database name: %s'%SETTINGS['database'])
    print('Database engine: %s'%SETTINGS['engine'])
    print('Table engine: %s'%SETTINGS['table_engine'])

