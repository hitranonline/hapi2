from setuptools import setup
from hapi2 import __version__

setup(
    name='hapi2',
    version=__version__,
    packages=[
        'hapi2',
        'hapi2.db',
        'hapi2.db.sqlalchemy',
        'hapi2.db.sqlalchemy.sqlite',
        'hapi2.db.sqlalchemy.mysql',
        
        'hapi2.lbl',
        'hapi2.web',
        'hapi2.format',
        'hapi2.format.hitran',
        'hapi2.format.streamers',
        
        'hapi2.config',
        
        'hapi2.collect',
        
        'hapi2.utils',        
        
        #'hapi2.metrics',
        #'hapi2.mixture',
        #'hapi2.partsum',
        #'hapi2.profile',
        #'hapi2.units',
        #'hapi2.xsec',
        #'hapi2.version',
        #'hapi2.proxy',
        #'hapi2.abscoef.numba.cpu',
        #'hapi2.abscoef',
        #'hapi2.visual',
        #'hapi2.quanta',
        #'hapi2.quanta.lbl',
        #'hapi2.quanta.hitran_cases',
    ],
    #license='BSD-2',
)
