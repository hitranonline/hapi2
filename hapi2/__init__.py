import os
import sys
import importlib

from .config import SETTINGS, VARSPACE

from .version import __version__
print('HAPI2 version: ',__version__)

# Import HAPI library (avoiding on-import stdout output).
__devnull__ = open(os.devnull, 'w')
__stdout__ = sys.stdout
sys.stdout = __devnull__
import hapi
sys.stdout = __stdout__

# Read config file.
from . import config
config.init()

# Initialize the database backend module.
from . import db
db.init()

# Initialize the web API module.
from . import web
web.init()

# Do higher-level imports.
from hapi2.collect import Collection
from hapi2.utils import tic,toc,tictoc
from hapi2.format.utils import read_header

db_backend = VARSPACE['db_backend']

__db_backend_objects__ = [
    'query',
    'Molecule','MoleculeAlias','Transition','Source',
    'SourceAlias','CrossSection','Transition','Isotopologue',
    'IsotopologueAlias','ParameterMeta','Linelist',
]
for _ in __db_backend_objects__:
    setattr(sys.modules[__name__], _, 
        getattr(db_backend.models, _))


__web_api_objects__ = [
    'fetch_cia','fetch_molecule_categories','fetch_parameter_metas',
    'fetch_transitions','fetch_isotopologues','fetch_cross_section_spectra',
    'fetch_cross_section_headers','fetch_sources','fetch_molecules',
    'fetch_info',
]
for _ in __web_api_objects__:
    setattr(sys.modules[__name__], _, 
        getattr(web.api, _))

