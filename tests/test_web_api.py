import os,sys
import json
import importlib
from time import time
from hapi2.collect import Collection, uuid
from hapi2 import Molecule
from pathlib import Path
from datetime import datetime

BASEDIR = '~tests'

def timeit(func,*args,**argv):
    t = time()
    res = func(*args,**argv)
    return time()-t, res

def create_dir(testgroup,session_uuid):    
    path = Path(BASEDIR,testgroup,session_uuid)
    path.mkdir(parents=True,exist_ok=True)

def save_test_results(testgroup,session_name,session_uuid,case,elapsed_time,test_results):
    """ Save test results for further processing and analyses.
        Results are supplied in Collection"""
    create_dir(testgroup,session_uuid)
    test_results.export_csv(os.path.join(BASEDIR,testgroup,session_uuid,case+'.csv'),)

def append_summary_collection(testgroup,session_name,session_uuid,case,elapsed_time,test_results):
    col = Collection()
    path_to_summary = os.path.join(BASEDIR,'summary.csv')
    if os.path.isfile(path_to_summary):
        col.import_csv(path_to_summary)
    item = {'testgroup':testgroup,'session_name':session_name,
        'session_uuid':session_uuid,'case':case,
        'elapsed_time':elapsed_time,'datetime':datetime.today()}
    col.update(item)
    col.export_csv(path_to_summary)

def runtest(func,testgroup='~testgroup',session_name='~session',session_uuid=None,save=False):
    
    if session_uuid is None:
        session_uuid = uuid()
    
    case = func.__name__
    
    print('\n============================================')
    print('============================================')    
    print(case)
    print('============================================')
    print('============================================')    
    
    elapsed_time,test_results = func()

    print('\n============================================')    
    print('ELAPSED TIME: %f sec.'%elapsed_time)   
    print('============================================\n') 
    
    print('ADDITIONAL RESULTS')
    test_results.head()

    if save:
        save_test_results(testgroup,session_name,session_uuid,case,elapsed_time,test_results)
        append_summary_collection(testgroup,session_name,session_uuid,case,elapsed_time,test_results)
    
    print('')

def fetch_objects(fetch_func,*args,**argv):    
    
    elapsed_time,objs = timeit(fetch_func,*args,**argv)    
    
    test_results = Collection()
    test_results.update([obj.dump() for obj in objs])    
    
    return elapsed_time,test_results    

def test_fetch_info():
    
    from hapi2 import fetch_info    
    
    elapsed_time,server_info = timeit(fetch_info)
    
    test_results = Collection()
    test_results.update([server_info])
    
    return elapsed_time,test_results
        
def test_fetch_parameter_metas_all():        
    from hapi2 import fetch_parameter_metas    
    return fetch_objects(fetch_parameter_metas)
    
def test_fetch_sources_all():
    from hapi2 import fetch_sources
    return fetch_objects(fetch_sources)

def test_fetch_molecules_all():
    from hapi2 import fetch_molecules
    return fetch_objects(fetch_molecules)

def test_fetch_isotopologues_all():
    from hapi2 import fetch_isotopologues
    mols = Molecule.all()
    return fetch_objects(fetch_isotopologues,mols)

def test_fetch_transitions_water_tiny():
    from hapi2 import fetch_transitions
    isos = Molecule('water').isotopologues
    return fetch_objects(fetch_transitions,isos,2000,2005,'water_tiny')

def test_fetch_transitions_water_normal():
    from hapi2 import fetch_transitions
    isos = Molecule('water').isotopologues
    return fetch_objects(fetch_transitions,isos,2000,2500,'water_normal')

def test_fetch_transitions_co2_large():
    from hapi2 import fetch_transitions
    isos = Molecule('co2').isotopologues
    return fetch_objects(fetch_transitions,isos,1000,3000,'co2_normal')

def test_fetch_cross_section_headers_ccl4():
    from hapi2 import fetch_cross_section_headers
    mol = Molecule('ccl4')
    return fetch_objects(fetch_cross_section_headers,mol)

def test_fetch_cross_section_spectra_ccl4():

    from hapi2 import fetch_cross_section_spectra
    
    xss = Molecule('ccl4').cross_sections
    elapsed_time,_ = timeit(fetch_cross_section_spectra,xss)
        
    test_results = Collection()
    
    return elapsed_time,test_results

TEST_CASES = [
    test_fetch_info,
    test_fetch_parameter_metas_all,
    test_fetch_sources_all,
    test_fetch_molecules_all,
    test_fetch_isotopologues_all,
    test_fetch_transitions_water_tiny,
    test_fetch_transitions_water_normal,
    test_fetch_transitions_co2_large,
    #test_fetch_cross_section_headers_ccl4,
    #test_fetch_cross_section_spectra_ccl4,
]

def do_tests(TEST_CASES,testgroup=None,session_name=None): # test all functions    

    if testgroup is None:
        testgroup = __file__

    session_uuid = uuid()
    
    for test_fun in TEST_CASES:        
        runtest(test_fun,testgroup,session_name,session_uuid,save=True)
        
if __name__=='__main__':
    
    try:
        session_name = sys.argv[1]
    except IndexError:
        session_name = '__not_supplied__'
        
    do_tests(TEST_CASES,session_name=session_name)
    
