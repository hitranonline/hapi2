from ..config import SETTINGS, VARSPACE

from .api import *

def init():

    print('Web API TODO: include fetch_info() into init')

    # Create temporary dir if doesn't exist
    if not os.path.isdir(SETTINGS['tmpdir']):
        os.mkdir(SETTINGS['tmpdir'])

    # Read server information if exists.
    server_info_file = 'server_info.json'
    if os.path.isfile(server_info_file):
        with open(server_info_file) as f:
            VARSPACE['server_info'] = json.load(f)
