import sys
from hapi2.collect import Collection, uuid
from hapi2 import Molecule

from unittests import timeit, runtest

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
    
