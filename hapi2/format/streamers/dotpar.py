"""
Streaming the first-generaton HAPI ".data" file format storing transitions
with "extended" (non-standard) sets of parameters.
"""

import os
import re
import json

from hapi2.format.streamer import AbstractStreamer

from ..hitran.lbl import HITRAN_DotparParser

def parse_hapi_line_(line,HAPI_HEADER,TYPES,par_line_flag=True):
    """
    THIS TEMPORARY VERSION ASSUMES THAT "ORDER" SECTION
    EITHER REPESENTS FULL "PAR_LINE" SET OR EMPTY.
    """   
    parts = line.rstrip().split(',')
    PARAMS = {}
    di = 0
    if par_line_flag:
        PARAMS['par_line'] = parts[0]
        di = 1
    for i,par in enumerate(HAPI_HEADER['extra']):
        i_ = i+di
        part = parts[i_]
        if parts[i_]=='#': 
            continue
        PARAMS[par] = TYPES[par](part)        
    return PARAMS

def prepare_type_table_(HAPI_HEADER):
    """
    Prepare fast lookup table for type conversions of HAPI parameters.
    """
    type_tokens = {'d':int, 'f':float, 'e':float, 's':str}
    TYPES = {}
    for par in HAPI_HEADER['extra']:
        fmt = HAPI_HEADER['extra_format'][par]
        token = re.search('%[\d\.]*([esfdESFD])',fmt).group(1)
        TYPES[par] = type_tokens[token.lower()]
    return TYPES

def get_iso_alias_(par_line):
    """
    Get isotopologue alias from par_line.
    """
    M = par_line[0:2]
    I = par_line[2:3]
    return 'HITRAN-iso-%s-%s'%(M,I)
    
# parameters which are doubled in direct object attributes
#TRANS_ATTRS = ['nu','sw','elower','gp','gpp']
#TRANS_ATTRS_PAR_LINE = set(['nu','sw','elower','gp','gpp']) # Transition attributes doubled in par line
TRANS_ATTRS = ['molec_id','local_iso_id','nu','sw','a','gamma_air','gamma_self','elower',
    'n_air','delta_air','global_upper_quanta','global_lower_quanta','local_upper_quanta',
    'local_lower_quanta','ierr','iref','line_mixing_flag','gp','gpp']
TRANS_ATTRS_PAR_LINE = set(TRANS_ATTRS)
 
def stream_hapi_transition_data_(tmpdir,filestem,par_line_flag=True):
    """
    Helper function streaming the HAPI .data file containing the information 
    about transitions.
    OUTPUT: lazy stream containing dicts with transition parameters.
    N.B. par_line_flag is True if the line in datafile starts with 160-char line,
         otherwise it is False.
    """
        
    header_full_path = os.path.join(tmpdir,filestem+'.header')
    data_full_path = os.path.join(tmpdir,filestem+'.data')
    
    # Read HAPI header
    with open(header_full_path) as f:
        HAPI_HEADER  = json.load(f)
        
    # Prepare type table for converting parameters
    TYPES = prepare_type_table_(HAPI_HEADER)
            
    # Iterate through the data file
    with open(data_full_path) as f:   
        for line in f:
            
            # get params from the line
            PARAMS = parse_hapi_line_(line,HAPI_HEADER,TYPES,par_line_flag)
            
            # create empty dictionary
            DCT = {}
            
            for attr in TRANS_ATTRS:
                if attr in PARAMS:
                    TRANS_ATTRS_PAR_LINE.remove(attr)
                    DCT[attr] = PARAMS[attr]
            
            # parameters from par_line which are doubled in direct object attributes

            if 'par_line' in PARAMS:
                parser = HITRAN_DotparParser(PARAMS['par_line'])
                flag_success = True
                for attr in TRANS_ATTRS_PAR_LINE:
                    try:
                        DCT[attr] = getattr(parser,attr)     
                    except ValueError as e:
                        print('\n!!! FAILED TO PARSE PAR_LINE (SKIPPING)>>>')
                        print(line)
                        print(e,'\n')
                        flag_success = False; break
                if not flag_success: continue
                #DCT['par_line'] = PARAMS.pop('par_line')
                del PARAMS['par_line']
                
            # Special parameters: id
            if 'trans_id' in PARAMS: DCT['id_'] = PARAMS.pop('trans_id')
            
            # Special parameters: try to establish isotopologue alias            
            if 'isotopologue_alias' in PARAMS:
                DCT['isotopologue_alias'] = PARAMS.pop('isotopologue_alias')
            elif 'iso_id' in PARAMS: 
                DCT['isotopologue_alias'] = 'HITRAN-iso-%d'%PARAMS.pop('iso_id')
            elif 'global_iso_id' in PARAMS: 
                DCT['isotopologue_alias'] = 'HITRAN-iso-%d'%PARAMS.pop('global_iso_id')
            elif 'molec_id' in DCT and 'local_iso_id' in DCT:
                DCT['isotopologue_alias'] = 'HITRAN-iso-%d-%d'%\
                    (DCT['molec_id'],DCT['local_iso_id'])
            #elif 'molec_id' in PARAMS and 'local_iso_id' in PARAMS:
            #    DCT['isotopologue_alias'] = 'HITRAN-iso-%d-%d'%\
            #        (PARAMS.pop('molec_id'),PARAMS.pop('local_iso_id'))
            #elif 'par_line' in PARAMS:
            #    DCT['isotopologue_alias'] = get_iso_alias_(PARAMS['par_line'])
            else:
                DCT['isotopologue_alias'] = 'unknown_alias'
            
            # All other parameters:
            DCT['extra'] = PARAMS
                        
            yield DCT

class DotparStreamer(AbstractStreamer):
    def __iter__(self):
        tmpdir = self.__basedir__
        filestem = self.__header__['content']['linelist']
        for item in stream_hapi_transition_data_(tmpdir,filestem,par_line_flag=True):
            yield item
