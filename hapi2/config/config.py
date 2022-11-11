import os
import sys
import json

DEFAULT_DATABASE_DIR = r'./'
CONFIG_FILE = 'config.json'

SETTINGS_DEFAULT = {

    # general settings
    'debug': True,

    # database settings
    'engine' : 'sqlite',
    'database' : 'local',
    'user': 'root',
    'pass': None,
    'database_dir' : DEFAULT_DATABASE_DIR,
    'echo': False,
    
    # "abscoef" settings
    
    # web api settings
    'api_version':'v2',
    'display_fetch_url':False,
    'proxy':None,
    'host': 'https://hitran.org',
    'tmpdir': '~tmp',
    'api_key': None,
    'info': 'server_info.json', # ?
}

SETTINGS = {}

VARSPACE = {
    'engine': None, 
    'session': None, 
}

def check_settings():
    if 'database' not in SETTINGS:
        raise Exception('\"database\" not in SETTINGS')
    if SETTINGS['database'] is None:
        raise Exception('SETTINGS[\'database\'] is None')
    if 'database_dir' not in SETTINGS:
        raise Exception('\"database_dir\" not in SETTINGS')
    if SETTINGS['database_dir'] is None:    
        raise Exception('SETTINGS[\'database_dir\'] is None')

def get_config_string():
    config_str = '[SETTINGS]'
    for key in SETTINGS_DEFAULT:
        config_str += '\n%s = %s'%(key,str(SETTINGS_DEFAULT[key]))
    return config_str

def print_config():
    config_str = get_config_string()
    print(config_str)
        
def save_config(config=None):
    """
    Print sample configuration file.
    """
    if not config:
        config = CONFIG_FILE
    if os.path.isfile(config):
        print('File \"%s\" already exists.'%config)
        return
    config_str = get_config_string()
    with open(config,'w') as f:
        f.write(config_str)

def print_settings():
    print(json.dumps(SETTINGS,indent=2))

def read_local_config(config=None):
    """
    Update the settings dictionary.
    """
    if not config:
        config = CONFIG_FILE

    if not os.path.isfile(config):
        print('Local configuration file (%s) not found.'%config)
        
        # Create and write default config file.
        with open(CONFIG_FILE,'w') as f:
            #print(json.dumps(SETTINGS_DEFAULT,indent=3))
            json.dump(SETTINGS_DEFAULT,f,indent=3)
        
        if SETTINGS_DEFAULT['api_key'] is None:
            print('WARNING: api key is empty. To use HAPI2, '
                'get the api key from your '
                'user profile on %s'%SETTINGS_DEFAULT['host'])        

    # Read configuration file and override default settings.
    with open(CONFIG_FILE) as f:
        dct = json.load(f)
        
    SETTINGS.update(dct)
    print('Updated SETTINGS_DEFAULT by local config file (%s).'%\
        os.path.abspath(config))

def init(**argv):
    """
    SETTINGS PRIORITY (FROM LOWEST TO HIGHEST):
    -> INITIAL VALUES FOR SETTINGS_DEFAULT  
      -> LOCAL CONFIG FILE ("xscdb.conf" by default)
        -> EDITING SETTINGS BEFORE CALLING START(ARGV**) 
          -> USER PARAMETERS SUPPLIED AS START(ARGV**) ARGUMENTS
    """
    # Read settings from the local configuration file.
    read_local_config()

    # Override settings by the function arguments.
    if argv:
        SETTINGS.update(argv)

    # Check settings for the most essential parameters.
    check_settings()
