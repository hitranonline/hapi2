import os
import json
from hapi2.config import SETTINGS

def read_header(filename,basedir=None):
    if not basedir: 
        basedir = SETTINGS['tmpdir']
    with open(os.path.join(basedir,filename)) as f:
        header = json.load(f)
    return header
        
