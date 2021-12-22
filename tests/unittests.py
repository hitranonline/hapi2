import os
import json
from time import time
from hapi2.collect import Collection, uuid
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
