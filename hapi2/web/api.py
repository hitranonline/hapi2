import os
import re
import sys
import json

from hapi2.config import SETTINGS, VARSPACE
db_backend = VARSPACE['db_backend']

#from ..formats import read_xsc
#from ..formats import HITRAN_DotparParser

import hapi
from hapi import prepareHeader

import uuid as uuidmod
def uuid():
    return str(uuidmod.UUID(bytes=os.urandom(16), version=4))

# Enable warning repetitions
from warnings import warn,simplefilter
simplefilter('always', UserWarning)

# Python 3 compatibility
import urllib
import urllib.request as urllib2
    
# Define open using Linux-style line endings
import io
def open_(*args,**argv):
    argv.update(dict(newline='\n'))
    return io.open(*args,**argv)

def Q_process_(val):
    """
    Process argument value and convert to string.
    """
    if type(val) in {str,int,float,bool}:
        return str(val)
    elif type(val) in {set,list,tuple}:
        return ','.join(str(v) for v in val)
    else:
        raise Exception('bad type for query: "%s"'%type(val))

class Q(object):
    """
    Django-style query class with limited functionality.    
    """
    def __init__(self,**argv):
        self.string = '&'.join(['%s=%s'%(key,Q_process_(argv[key])) for key in argv])        
        
    def __and__(self,q):
        q = Q()
        q.string = self.string+'&'+q.string
        return q
            
def fetch_header(api_section,query=None,need_headfile=False):
    # check if api key is supplied
    if not SETTINGS['api_key']:
        raise Exception('No api key specified in config file. '
            'Get api key from the user profile on HITRANonline (https://hitran.org)')
    # create URL
    print('\nHeader for %s is fetched from %s\n'%(api_section,SETTINGS['host']))
    url = '{host}/api/{api_version}/{api_key}/{api_section}'.format(
        host=SETTINGS['host'].strip('/'),
        api_version=SETTINGS['api_version'],
        api_key=SETTINGS['api_key'],
        api_section=api_section)
    if query is not None: url += '?%s'%query.string # not encoded
    #if query is not None: url += '?%s'%urllib.parse.quote(query.string) # encoded
    # Download data by chunks.
    if SETTINGS['display_fetch_url']: print(url+'\n')
    try:
        # Proxy handling # https://stackoverflow.com/questions/1450132/proxy-with-urllib2
        if SETTINGS['proxy']:
            print('Using proxy '+str(SETTINGS['proxy']))
            proxy = urllib2.ProxyHandler(SETTINGS['proxy'])
            opener = urllib2.build_opener(proxy)
            urllib2.install_opener(opener)            
        req = urllib2.urlopen(url)
    except urllib2.HTTPError:
        raise Exception('Failed to retrieve data for given parameters.')
    #except urllib2.URLError:
    #    raise Exception('Cannot connect to %s. Try again or edit host variable.' % SETTINGS['host'])
    #CHUNK = 64 * 1024
    CHUNK = 1024 * 1024
    print('BEGIN DOWNLOAD: '+api_section)
    headfile = os.path.join(SETTINGS['tmpdir'],uuid()+'.json')
    with open_(headfile,'w') as fp:
       while True:
          chunk = req.read(CHUNK)
          if not chunk: break
          fp.write(chunk.decode('utf-8'))
          print('  %d bytes written to %s' % (CHUNK,headfile))
    print('END DOWNLOAD')
    with open_(os.path.join(headfile)) as fp:
        HEADER = json.load(fp)
    print('PROCESSED')
    if need_headfile:
        return HEADER,headfile
    else:
        return HEADER

def fetch_file(prefix,filename_server,filename_local=None):
    if filename_local is None: filename_local = filename_server
    print('\nFile %s is fetched from %s\n'%(filename_server,prefix))
    url = '{host}/{prefix}/{filename}'.format(
        host=SETTINGS['host'].strip('/'),
        prefix=prefix,filename=filename_server)
    # Download data by chunks.
    if SETTINGS['display_fetch_url']: print(url+'\n')
    try:
        # Proxy handling # https://stackoverflow.com/questions/1450132/proxy-with-urllib2
        if SETTINGS['proxy']:
            print('Using proxy '+str(SETTINGS['proxy']))
            proxy = urllib2.ProxyHandler(SETTINGS['proxy'])
            opener = urllib2.build_opener(proxy)
            urllib2.install_opener(opener)            
        req = urllib2.urlopen(url)
    except urllib2.HTTPError:
        raise Exception('Failed to retrieve data for given parameters.')
    #except urllib2.URLError:
    #    raise Exception('Cannot connect to %s. Try again or edit host variable.' % SETTINGS['host'])
    #CHUNK = 64 * 1024
    #CHUNK = 4 * 1024 * 1024
    CHUNK = 64 * 1024 * 1024
    print('BEGIN DOWNLOAD: '+filename_server)
    headfile = os.path.join(SETTINGS['tmpdir'],filename_local)
    with open_(headfile,'w') as fp:
       while True:
          chunk = req.read(CHUNK)
          if not chunk: break
          fp.write(chunk.decode('utf-8'))
          print('  %d bytes written to %s' % (CHUNK,headfile))
    print('END DOWNLOAD')
    #with open_(os.path.join(headfile)) as fp:
    #    HEADER = json.load(fp)
    print('PROCESSED')
    #return HEADER    
    return headfile

def save_hapi_header(HAPI1_HEADER,HEADER_TRANSITIONS):
    tmpdir = SETTINGS['tmpdir']
    llst_name = HEADER_TRANSITIONS['content']['linelist']
    headfile = llst_name+'.header'
    print('saving HAPI header %s to %s'%(headfile,tmpdir))
    headfile = os.path.join(tmpdir,headfile)
    with open_(headfile,'w') as fp:
        json.dump(HAPI1_HEADER,fp)
            
def fetch_info():
    """
    Fetch general information from the server.
    """
    HEADER = fetch_header('info')
    
    VARSPACE['server_info'] = HEADER
    
    return HEADER
    
def fetch_molecules():
    """
    Fetch molecule list (simple).
    """
    HEADER = fetch_header('molecules')
    
    mols = db_backend.models.Molecule.update(HEADER,local=False)
        
    return mols.all()

def fetch_collision_complexes():
    """
    Fetch molecule list (simple).
    """
    HEADER = fetch_header('collision-complexes')
    
    ccomps = db_backend.models.CollisionComplex.update(HEADER,local=False)
        
    return ccomps.all()
    
def fetch_sources(ids=None):
    """
    Fetch source headers using the ID list.
    """
    if ids is None:
        query = None
    else:
        query = Q(id__in=ids)
    
    HEADER = fetch_header('sources',query)
    
    srcs = db_backend.models.Source.update(HEADER,local=False)

    return srcs.all()
        
def fetch_cross_section_headers(mols):
    """
    Fetch cross-section headers using molecules as in input.
    """    
    if type(mols) not in [list,tuple]:
        mols = [mols]
        
    # get the ids of the molecules
    ids = [mol.id for mol in mols]
    # form and query
    query = Q(molecule_id__in=ids)
            
    # Step 1: request cross-sections, but don't save them
    HEADER = fetch_header('cross-sections',query)
            
    # Step 3: Finally, save the cross-sections
    xss = db_backend.models.CrossSection.update(HEADER,local=False)

    return xss.all()
    
def attach_data_to_cross_sections(xss,datadir,local=True): # TEMPORARY VERSION
    """
    Attach the spectral data to cross section objects.
    The data should be in the datadir folder. 
    """
    from hapi2.format.hitran.xsc import read_xsc
    for xs in xss:
        xsc,_ = read_xsc(datadir,xs.filename)
        xs.set_data(nu=None,xsc=xsc)
        #xs.save()
    VARSPACE['session'].commit()
                
def fetch_cross_section_spectra(xss):
    """
    Fetch actual spectra using the pre-fetched headers.
    """
    for xs in xss:
        fetch_file('data/xsec',xs.filename)

    attach_data_to_cross_sections(xss,SETTINGS['tmpdir'])
    # TODO: attach_data back to the updaters.
    VARSPACE['session'].commit()

def fetch_cross_sections(mols):
    """
    Fetch cross-section headers and data.
    """    
    xss = fetch_cross_section_headers(mols)
    fetch_cross_section_spectra(xss)
    # TODO: move attach_data back to the updaters.
    return xss

def fetch_cia_cross_section_headers(ccomps):
    """
    Fetch cross-section headers using collision complexes as in input.
    """    
    if type(ccomps) not in [list,tuple]:
        ccomps = [ccomps]
        
    # get the ids of the molecules
    ids = [ccomp.id for ccomp in ccomps]
    # form and query
    query = Q(collision_complex_id__in=ids)
            
    # Step 1: request cross-sections, but don't save them
    HEADER = fetch_header('cia-cross-sections',query)
            
    # Step 3: Finally, save the cross-sections
    xss = db_backend.models.CIACrossSection.update(HEADER,local=False)

    return xss.all()

def cia_header_signature(xs):
    """ Calculate signature cor CIA object for search """
    fields = ['numin','numax','npnts','temperature','cia_max',
        'resolution','comment','local_ref_id',]
    signature = tuple([xs.collision_complex.chemical_symbol]+\
        [getattr(xs,field)for field in fields])
    return signature

def attach_data_to_cia_cross_sections(xss,datadir,local=True): # TEMPORARY VERSION
    """
    Attach the spectral data to cross section objects.
    The data should be in the datadir folder. 
    """
    from hapi2.format.hitran.cia import parse
    XSC_BUF = {}
    for xs in xss:
        filename = xs.filename
        # prepare lookup buffer
        if filename not in XSC_BUF:
            print('parsing %s'%filename)
            XSC_BUF[filename] = {}
            xss_ = parse(os.path.join(datadir,filename),silent=False)
            for xs_ in xss_:
                head_sign = cia_header_signature(xs_)
                if head_sign in XSC_BUF[filename]:
                    raise Exception('signature %s is already in %s'%\
                        (head_sign,filename))
                XSC_BUF[filename][head_sign] = xs_
        # find needed temperature
        data = XSC_BUF[filename][cia_header_signature(xs)].data
        xs.set_data(nu=data['nu'],xsc=data['xsc'])
        #xs.save()
    VARSPACE['session'].commit()
                
def fetch_cia_cross_section_spectra(xss):
    """
    Fetch actual spectra using the pre-fetched headers.
    """
    FETCHED = set()
    for xs in xss:
        if xs.status=='main':
            global_path = 'data/CIA'
        elif xs.status=='alternate':
            global_path = 'data/CIA/supplementary'
        else:
            raise Exception('unknown cross-section status: "%s"'%xs.status)
        filename = xs.filename
        if filename not in FETCHED:
            fetch_file(global_path,filename)
            FETCHED.add(filename)

    attach_data_to_cia_cross_sections(xss,SETTINGS['tmpdir'])
    # TODO: attach_data back to the updaters.
    VARSPACE['session'].commit()

def fetch_cia_cross_sections(ccomps):
    """
    Fetch CIA cross-section headers and data.
    """    
    xss = fetch_cia_cross_section_headers(ccomps)
    fetch_cia_cross_section_spectra(xss)
    # TODO: move attach_data back to the updaters.
    return xss

def fetch_isotopologues(mols):
    """
    Fetch isotopologues using molecule aliases as an input.
    """
    if type(mols) not in [list,tuple]:
        mols = [mols]
    
    # get the ids of the molecules
    ids = [mol.id for mol in mols]
    
    # get isotopologues
    HEADER = fetch_header('isotopologues',Q(molecule_id__in=ids))

    # update and commit
    isos = db_backend.models.Isotopologue.update(HEADER,local=False)
        
    return isos.all()
    
def fetch_parameter_metas(pattern=None):
    """
    Fetch molecule alias headers using substring patterns.
    """
    if pattern is None:
        query = None
    else:
        query = Q(name__icontains=pattern)
    
    HEADER = fetch_header('parameter-metas',query)
    
    pmetas = db_backend.models.ParameterMeta.update(HEADER,local=False)
    
    return pmetas.all()    
    
def fetch_molecule_categories(pattern=None):
    """
    Fetch molecule category headers using substring patterns.
    """
    pass
    
def fetch_transitions(isos,numin,numax,llst_name):
    """
    Fetch transitions using isotopologue objects as an input.
    """
    if type(isos) not in [list,tuple]:
        isos = [isos]
        
    # Check if server info in downloaded.
    if 'server_info' not in VARSPACE:
        fetch_info()
        
    # Check if linelist exists.
    if db_backend.models.Linelist(llst_name) in VARSPACE['session']:
        raise Exception('linelist %s already exists'%llst_name)
    
    # get the ids of the isotopologues
    ids = [iso.id for iso in isos]
    
    # PARLIST
    parlist = ['par_line','trans_id','global_iso_id']
    
    # create a query object
    query = Q(iso_ids_list=ids,numin=numin,numax=numax,
        head=False,fixwidth=0,request_params=parlist) # add parameter lists in later release
    
    # get transitions
    HEADER_TRANSITIONS,headfile = fetch_header('transitions',query,need_headfile=True)
    HEADER_TRANSITIONS['content']['linelist'] = llst_name # save linelist to a same file each time
    
    # re-save header to store the linelist information
    with open(headfile,'w') as f:
        json.dump(HEADER_TRANSITIONS,f)
    
    # get transition file
    filename = HEADER_TRANSITIONS['content']['data']
    prefix = VARSPACE['server_info']['content']['data']['results_dir']

    fetch_file(prefix,filename,llst_name+'.data')
    
    # prepare and save HAPI header for transitions.
    HAPI1_HEADER = prepareHeader(parlist=parlist) # add parameter list in later release
    save_hapi_header(HAPI1_HEADER,HEADER_TRANSITIONS)

    # update and commit
    #transs = update_and_commit_transitions_plain_core(HEADER_TRANSITIONS,llst_name)
    transs = db_backend.models.Transition.update(HEADER_TRANSITIONS,local=False,llst_name=llst_name)
        
    return transs

TIPS_LOOKUP_TABLE = {
    '2011': {
        'ISOT_HASH':lambda M,I: hapi.Tdat, 
        'ISOQ_HASH':lambda M,I: hapi.TIPS_ISO_HASH[(M,I)], 
        'SOURCE_DOI':'10.1016/j.icarus.2011.06.004'
    },
    '2017': {
        'ISOT_HASH':lambda M,I: hapi.TIPS_2017_ISOT_HASH[(M,I)], 
        'ISOQ_HASH':lambda M,I: hapi.TIPS_2017_ISOQ_HASH[(M,I)], 
        'SOURCE_DOI':'10.1016/j.jqsrt.2017.03.045'
    },
    '2021': {
        'ISOT_HASH':lambda M,I: hapi.TIPS_2021_ISOT_HASH[(M,I)], 
        'ISOQ_HASH':lambda M,I: hapi.TIPS_2021_ISOQ_HASH[(M,I)],
        'SOURCE_DOI':'10.1016/j.jqsrt.2021.107713'
    },
}

def get_MI(iso):
    """ Get local numbers of molecule and isotopologue.
        !!! ATTENTION: If isoid=0 encountered, it will 
        be automatically translated to iso=10 """
    M = iso.molecule.id
    I = iso.isoid
    if I==0: I=10; # !!!
    return M,I

def attach_data_to_pfuncs_from_hapi(pfuncs):
    """ Attach data from HAPI v1 to partition function objects"""
    
    for pfunc in pfuncs:
        PFUNC_JSON = json.loads(pfunc.json)
        TIPS_VERSION = PFUNC_JSON['TIPS_VERSION']
        TIPS_LOOKUP = TIPS_LOOKUP_TABLE[TIPS_VERSION]
        M,I = get_MI(pfunc.isotopologue)
        TT = TIPS_LOOKUP['ISOT_HASH'](M,I)
        QQ = TIPS_LOOKUP['ISOQ_HASH'](M,I)
        pfunc.set_data(TT,QQ)

def get_local_pfunc_id(tips_version,iso):
    ID_LOOKUP = {
        '2011':1000000,
        '2017':2000000,
        '2021':3000000,
    }
    Mid = 100*iso.molecule.id
    Iid = iso.isoid
    return ID_LOOKUP[tips_version]+Mid+Iid

def get_local_pfunc_q296(TT,QQ):
    import numpy as np
    #    i = TT.index(296)
    #    return QQ[i]
    #except ValueError:
    #    return None
    inds = np.where(TT==296.0)[0]
    if len(inds)==0: return None
    return QQ[inds[0]]

def get_query_header_from_hapi(isos):
    """ Get dummy query header for partition functions in HAPI v1 """
    
    from datetime import datetime
    
    HEADER = {
        'status':'OK',
        'message':'',
        'content':{
            'class':'PartitionFunction',
            'format':'json',
            'data':[],
        },
        'timestamp':str(datetime.now()),
        'query':'',
        'source':'HAPI',
    }
    
    ITEM_TEMPLATE = {
        'id': None,                  
        'isotopologue_alias': None,
        'source_alias': None,
        'tmin': None,
        'tmax': None,
        'json': None,
        'filename': '',
        'comment': 'HAPI LOCAL',
        'status': 'main',
    }
    
    for iso in isos:
        for tips_version in TIPS_LOOKUP_TABLE:
            TIPS_LOOKUP = TIPS_LOOKUP_TABLE[tips_version]
            M,I = get_MI(iso)
            try:
                TT = TIPS_LOOKUP['ISOT_HASH'](M,I)
                QQ = TIPS_LOOKUP['ISOQ_HASH'](M,I)
            except KeyError:
                continue
            ITEM = ITEM_TEMPLATE.copy()
            ITEM['id'] = get_local_pfunc_id(tips_version,iso)
            ITEM['isotopologue_alias'] = 'HITRAN-iso-%d'%iso.id
            ITEM['source_alias'] = TIPS_LOOKUP['SOURCE_DOI']
            ITEM['tmin'] = min(TT)
            ITEM['tmax'] = max(TT)
            ITEM['json'] = json.dumps({'TIPS_VERSION':tips_version})
            HEADER['content']['data'].append(ITEM)
       
    #with open('tips_header.json','w') as f:
    #    f.write(json.dumps(HEADER))
    #raise Exception('debug')
       
    return HEADER

def fetch_partition_functions_tmp(isos):
    """
    Emulate fetching partition functions using local data from HAPI v1.
    """

    if type(isos) not in [list,tuple]:
        isos = [isos]
        
    # get dummy query header from HAPI v1
    HEADER = get_query_header_from_hapi(isos)
            
    # save the partition functions
    pfuncs = db_backend.models.PartitionFunction.update(HEADER,local=False)
    pfuncs = pfuncs.all()

    # attach data to partition functions
    attach_data_to_pfuncs_from_hapi(pfuncs)
    VARSPACE['session'].commit()
    
    return pfuncs

def fetch_partition_functions(isos):
    """
    Fetch partition functions using isotopologue objects as an input.
    """
    return fetch_partition_functions_tmp(isos)
