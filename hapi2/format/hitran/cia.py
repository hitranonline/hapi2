import os 

# parser for the collision-induced absorption

"""
CIA CROSS-SECTIONS ARE DESCRIBED IN:
[1] Richard et al. JSQRT 2012;113:1276-85. doi:10.1016/j.jqsrt.2011.11.004.
[2] Gordon et al. JQSRT 2017;130:4-50. doi:10.1016/j.jqsrt.2017.06.038.
    
CIA HEADER FORMAT (Ref. [1] is incorrect; use the readme file instead: http://hitran.org/data/CIA/CIA_Readme.pdf)
              H2-CH4     0.020  1946.000   1974   40.0 6.926E-43 -.999                Equilibrium 16
___________________!_________!_________!______!______!_________!_____!__________________________!__!              
chemical_symbol     wnmin     wnmax     npnts  T      ciamax    res   comment                    ref
20                  10        10        7      7      10        6     27                         3
0:20                20:30     30:40     40:47  47:54  54:64     64:70 70:97                      97:100
"""

class CollisionComplex_:
    def __init__(self,chemical_symbol):
        self.chemical_symbol = chemical_symbol

class CIA_():
    def __init__(self,header):
        self.data = {'nu':[],'xsc':[]}
        self.parse_header(header)

    def parse_header(self,header):
        # get cross-section in cm5/molecule2
        #self.chemical_symbol = header[0:20].strip()
        self.collision_complex = CollisionComplex_(header[0:20].strip())
        self.numin = float(header[20:30])
        self.numax = float(header[30:40])
        self.npnts = int(header[40:47])
        self.temperature = float(header[47:54])
        self.cia_max = float(header[54:64])
        self.resolution = float(header[64:70])
        self.comment = header[70:97]
        self.local_ref_id = int(header[97:100])
        self.filename = None
        
    def append_data(self,nu,xsc):
        # append single pair (nu,xsc) to cross-sections
        self.data['nu'].append(nu)
        self.data['xsc'].append(xsc)
        
    def set_data(self,nu,xsc):
        # set the nu and xsc data to the cross-section
        self.data = {'nu':nu,'xsc':xsc}
        
    def get_xsc(self):
        # get absorption cross-section in cm5/molecule2
        return self.data['nu'],self.data['xsc']
        
    def get_abscoef(self):
        # get absorption coefficient in cm-1
        pass

def parse(filepath,silent=False):
    """
    Parse CIA cross-section from the file.
    Return the Python dictionary containing all parameters.
    Assumes there are multiple cross-section T-sets inside the file.
    """
    f = open(filepath)
    head_flag = True
    xss = []
    for line in f:
        if not line.strip(): continue
        if head_flag: # current line is a header
            if not silent: print(line.rstrip())
            xsc = CIA_(line)
            filename = os.path.basename(filepath)
            xsc.filename = filename
            xss.append(xsc)
            #if not silent: print('======')            
            #import json
            #if not silent: print(json.dumps(xsc.__dict__,indent=2))
            head_flag = False  
            line_counter = 1            
        else: # current line can be a (nu,xsc) pair or a header
            if line_counter==xsc.npnts: # it is a last part for this cross-section and the next line is a header
                head_flag = True                  
            x,y = line.split()[:2]; x = float(x); y = float(y)
            xsc.append_data(x,y)
            line_counter += 1
    return xss
    
