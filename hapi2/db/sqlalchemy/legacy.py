import numpy as np
from .base import query as base_query
from hapi import LOCAL_TABLE_CACHE, HITRAN_DEFAULT_HEADER
from hapi2.config import VARSPACE

def storage2cache(tablename,query=None,parnames=None):
    """ Import transitions from database to RAM (legacy) """

    Transition = VARSPACE['db_backend'].models.Transition
    Linelist = VARSPACE['db_backend'].models.Linelist

    # query selector
    if query is None:
        q = Linelist(tablename).transitions
    else:
        q = query
        
    if parnames is None:
        parnames = [parname for parname,_ in Transition.__keys__]

    # exlcude 'extra' from pars
    parnames = set(parnames)-set(['extra'])
            
    data = q.with_entities(
        *[getattr(Transition,parname) for parname in parnames]
    ).all()
    
    data = list(zip(*data))
        
    LOCAL_TABLE_CACHE[tablename] = {}
    LOCAL_TABLE_CACHE[tablename]['header'] = HITRAN_DEFAULT_HEADER
    LOCAL_TABLE_CACHE[tablename]['data'] = {}
    
    for parname,datum in zip(parnames,data):
        LOCAL_TABLE_CACHE[tablename]['data'][parname] = np.array(datum)
