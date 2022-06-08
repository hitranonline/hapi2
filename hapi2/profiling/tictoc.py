""" Simple nested timers for benchmarking purposes """

import sys
from time import time

__SETTINGS__ = {'flag':True}
__BUFFER__ = {}

def set_flag(flag):
    __SETTINGS__['flag'] = flag

def tic(name):
    """
    Starts timer for the event with specified name.
    """
    if __SETTINGS__['flag']: 
        __BUFFER__['name'] = time()
    
def toc(name):
    """
    Stops the timer for the most recently started event and prints result.
    """
    if __SETTINGS__['flag']:         
        caller = sys._getframe(1)  # Obtain calling frame
        caller_name =  caller.f_globals['__name__'].upper()
        print('<<%s>>: %f sec. elapsed for %s'%\
            (caller_name,time()-__BUFFER__['name'],name))
    
def tictoc(name,active=True):
    """
    A simple decorator with parameter. Good for benchmarking functions.
    https://stackoverflow.com/questions/5929107/decorators-with-parameters
    """
    def decorator(foo):
        def wrapper(*argc,**argv):
            if active: tic(name)
            res = foo(*argc,**argv)
            if active: toc(name)
            return res
        return wrapper
    return decorator
