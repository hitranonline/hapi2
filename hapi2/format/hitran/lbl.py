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
        global_upper_quanta = lambda par_line:        par_line[ 67: 82]  ,
        global_lower_quanta = lambda par_line:        par_line[ 82: 97]  ,
        local_upper_quanta  = lambda par_line:        par_line[ 97:112]  ,
        local_lower_quanta  = lambda par_line:        par_line[112:127]  ,
        ierr                = lambda par_line:        par_line[127:133]  ,
        iref                = lambda par_line:        par_line[133:145]  ,
        line_mixing_flag    = lambda par_line:        par_line[145:146]  ,
        gp                  = lambda par_line: int( float( par_line[146:153] ) ),
        gpp                 = lambda par_line: int( float( par_line[153:160] ) ),  
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
