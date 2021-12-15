# original is taken from D:\work\Activities\CROSS-SECTIONS\GLOBAL_DATABASE\COMPARISONS\hitran_utils.py

import os
import numpy as np

STRICT = False

def to_type(val,type=float,mode=STRICT):
    try:
        return type(val)
    except ValueError as e:
        if STRICT:
            return val
        else:
            #raise Exception(e)
            raise
            
def parse_header(header_line):
    """
    Parse cross section header given in semi-HIRTAN format.
    EXAMPLE:
    "               SO2F2  599.9980 1999.2904  23220  296.0 700.0                  Andersen et al. (2009)"
    """
    HEADER = dict(
        Molecule = {
            "ChemicalFormula": to_type(header_line[0:20].strip(),str), 
            "CommonName": None
        },
        numin = to_type(header_line[20:30].strip(),float),
        numax = to_type(header_line[30:40].strip(),float),
        npnts = to_type(header_line[40:47].strip(),int),
        Temperature = to_type(header_line[47:54].strip(),float),
        Pressure = to_type(header_line[54:60].strip(),float),
        ReferenceAlias = to_type(header_line[60:100].strip(),str)
    )
    return HEADER
    
def read_xsc(dirname,filename):
    """ 
    Read cross section in HITRAN format.
    The file must contain only one T,p-set.
    """
    absolute_filename = os.path.join(dirname,filename)
    f = open(absolute_filename)
    header_line = f.readline()
    HEADER = parse_header(header_line)
    HEADER['SourceFile'] = filename
    npnts = HEADER['npnts']
    sigma = []
    for i in range(npnts):
        if not i%10:
            line = f.readline().rstrip()
            vals = line.split()
        sigma.append(float(vals[i%10]))
    print('\nRead %d points from %s'%(npnts,absolute_filename))
    return np.array(sigma),HEADER

def parse_header_new(header_line): # for the multi-header file, accounts for the main objects 
    """
    Parse cross section header given in semi-HIRTAN format.
    EXAMPLE:
    "               SO2F2  599.9980 1999.2904  23220  296.0 700.0                  Andersen et al. (2009)"
    """
    HEADER = dict(
        molecule_aliases = [header_line[0:20].strip(), header_line[75:90].strip()],
        numin = to_type(header_line[20:30].strip(),float),
        numax = to_type(header_line[30:40].strip(),float),
        npnts = to_type(header_line[40:47].strip(),int),
        temperature = to_type(header_line[47:54].strip(),float),
        pressure = to_type(header_line[54:60].strip(),float),
        broadener = header_line[90:97].strip(),
        sigma_max = to_type(header_line[60:70].strip(),float),
        local_ref_id = to_type(header_line[97:100].strip(),int),
    )
    return HEADER
    
def read_xsc_multi(dirname,filename):
    """ 
    Read cross section in HITRAN format.
    The file can contain multiple T,p-sets.
    """
    import textwrap # standard library for text wrapping
    wrapper = textwrap.TextWrapper(width=10,replace_whitespace=False,drop_whitespace=False) # create a wrapper since the default one is way too illogical
    absolute_filename = os.path.join(dirname,filename)
    f = open(absolute_filename)
    data = []
    while True:
        header_line = f.readline()
        if not header_line: 
            break
        HEADER = parse_header_new(header_line)
        HEADER['srcfile'] = filename
        HEADER['srcformat'] = 'xsc'
        npnts = HEADER['npnts']
        numin = HEADER['numin']
        numax = HEADER['numax']
        sigma = []
        for i in range(npnts):
            if not i%10:
                line = f.readline().rstrip()
                #vals = line.split()# doesn't work if values are glued together; each value must have 10 characters!
                vals = wrapper.wrap(line) # split line to parts 10 characters each
            sigma.append(float(vals[i%10]))
        print('\nRead %d points from %s for %s'%(npnts,absolute_filename,header_line))
        HEADER['data'] = {'sigma':np.array(sigma),'nu':np.linspace(numin,numax,npnts)}
        data.append(HEADER)
    return data

def parse_iso(par_line):
    """
    Parse iso token accounting for non-numerical entries (A,B,...)    
    """
    token = par_line[2:3]
    if '1'<=token<='9':
        return int(token)
    elif token=='0':
        return 10
    else:
        return 11+ord(token)-ord('A')

class HITRAN_DotparParser:
    """
    Simple parser of the HITRAN dotpar format.
    """
    PARAMETER_MAP = dict(
        molec_id            = lambda par_line: int(   par_line[  0:  2] ),
        local_iso_id        = lambda par_line: parse_iso(par_line),
        nu                  = lambda par_line: float( par_line[  3: 15] ),
        sw                  = lambda par_line: float( par_line[ 15: 25] ),
        a                   = lambda par_line: float( par_line[ 25: 35] ),
        gamma_air           = lambda par_line: float( par_line[ 35: 40] ),
        gamma_self          = lambda par_line: float( par_line[ 40: 45] ),
        elower              = lambda par_line: float( par_line[ 45: 55] ),
        n_air               = lambda par_line: float( par_line[ 55: 59] ),
        delta_air           = lambda par_line: float( par_line[ 59: 67] ),
        global_upper_quanta = lambda par_line: str(   par_line[ 67: 82] ),
        global_lower_quanta = lambda par_line: str(   par_line[ 82: 97] ),
        local_upper_quanta  = lambda par_line: str(   par_line[ 97:112] ),
        local_lower_quanta  = lambda par_line: str(   par_line[112:127] ),
        ierr                = lambda par_line: str(   par_line[127:133] ),
        iref                = lambda par_line: str(   par_line[133:145] ),
        line_mixing_flag    = lambda par_line: str(   par_line[145:146] ),
        gp                  = lambda par_line: float( par_line[146:153] ),
        gpp                 = lambda par_line: float( par_line[153:160] ),  
    )    
    def __init__(self,par_line):
        self.par_line = par_line
        
    def __getattr__(self,attr):
        if attr in self.PARAMETER_MAP:
            return self.PARAMETER_MAP[attr](self.par_line)
        else:
            raise Exception('invalid attribute for HITRAN_DotparParser: %s'%attr)   
    
    def all(self):
        return {
            pname:self.PARAMETER_MAP[pname](self.par_line) for pname in self.PARAMETER_MAP.keys()
        }

class HITRAN_DotparParser_2:
    """
    Simple parser of the HITRAN dotpar format.
    """
    PARAMETER_MAP = dict(
        molec_id     = lambda par_line: int(   par_line[  0:  2] ),
        #local_iso_id = lambda par_line: int(   par_line[  2:  3] ),
        local_iso_id = lambda par_line: parse_iso(par_line),
        nu           = lambda par_line: float( par_line[  3: 15] ),
        sw           = lambda par_line: float( par_line[ 15: 25] ),
        a            = lambda par_line: float( par_line[ 25: 35] ),
        gamma_air    = lambda par_line: float( par_line[ 35: 40] ),
        gamma_self   = lambda par_line: float( par_line[ 40: 45] ),
        elower       = lambda par_line: float( par_line[ 45: 55] ),
        n_air        = lambda par_line: float( par_line[ 55: 59] ),
        delta_air    = lambda par_line: float( par_line[ 59: 67] ),
        Qp           = lambda par_line: str(   par_line[ 67: 82] ),
        Qpp          = lambda par_line: str(   par_line[ 82: 97] ),
        qp           = lambda par_line: str(   par_line[ 97:112] ),
        qpp          = lambda par_line: str(   par_line[112:127] ),
        err_nu       = lambda par_line: int(   par_line[127:128] ),
        err_S        = lambda par_line: int(   par_line[128:129] ),
        err_gair     = lambda par_line: int(   par_line[129:130] ),
        err_gself    = lambda par_line: int(   par_line[130:131] ),
        err_nair     = lambda par_line: int(   par_line[131:132] ),
        err_dair     = lambda par_line: int(   par_line[132:133] ),
        ref_nu       = lambda par_line: int(   par_line[133:135] ),
        ref_S        = lambda par_line: int(   par_line[135:137] ),
        ref_gair     = lambda par_line: int(   par_line[137:139] ),
        ref_gself    = lambda par_line: int(   par_line[139:141] ),
        ref_nair     = lambda par_line: int(   par_line[141:143] ),
        ref_dair     = lambda par_line: int(   par_line[143:145] ),
        lm_flag      = lambda par_line: str(   par_line[145:146] ),
        gp           = lambda par_line: float( par_line[146:153] ),
        gpp          = lambda par_line: float( par_line[153:160] ),  
    )    
    def __init__(self,par_line):
        self.par_line = par_line
        
    def __getattr__(self,attr):
        if attr in self.PARAMETER_MAP:
            return self.PARAMETER_MAP[attr](self.par_line)
        else:
            raise Exception('invalid attribute for HITRAN_DotparParser: %s'%attr)   
    
    def all(self):
        return {
            pname:self.PARAMETER_MAP[pname](self.par_line) for pname in self.PARAMETER_MAP.keys()
        }
        
def dotpar_item_to_list(item): # TODO: UPDATE PARAMETER NAMES
    elements = ('M','I','nu','S','A','gair','gself','E_','nair','dair',
    'Q','Q_','q','q_','err_nu','err_S','err_gair',
    'err_gself','err_nair','err_dair','ref_nu',
    'ref_S','ref_gair','ref_gself','ref_nair',
    'ref_dair','g','g_')
    lst = [item[e] for e in elements]
    return lst

def load_dotpar(line):
    """
    Get the "raw" .par line on input
    and output the dictionary of parameters.
    """
    item = dict(
        molec_id   = int(   line[  0:  2] ),
        iso_id     = int(   line[  2:  3] ),
        nu         = float( line[  3: 15] ),
        sw         = float( line[ 15: 25] ),
        a          = float( line[ 25: 35] ),
        gamma_air  = float( line[ 35: 40] ),
        gamma_self = float( line[ 40: 45] ),
        elower     = float( line[ 45: 55] ),
        n_air      = float( line[ 55: 59] ),
        delta_air  = float( line[ 59: 67] ),
        Q         = str(   line[ 67: 82] ),
        Q_        = str(   line[ 82: 97] ),
        q         = str(   line[ 97:112] ),
        q_        = str(   line[112:127] ),
        err_nu    = int(   line[127:128] ),
        err_S     = int(   line[128:129] ),
        err_gair  = int(   line[129:130] ),
        err_gself = int(   line[130:131] ),
        err_nair  = int(   line[131:132] ),
        err_dair  = int(   line[132:133] ),
        ref_nu    = int(   line[133:135] ),
        ref_S     = int(   line[135:137] ),
        ref_gair  = int(   line[137:139] ),
        ref_gself = int(   line[139:141] ),
        ref_nair  = int(   line[141:143] ),
        ref_dair  = int(   line[143:145] ),
        g         = float( line[145:153] ),
        g_        = float( line[153:160] ),  
    )   
    return item