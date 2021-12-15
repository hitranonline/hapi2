import os
import numpy as np

def parse_header(header_line):
    """
    Parse cross section header given in semi-HIRTAN format.
    EXAMPLE:
    "               SO2F2  599.9980 1999.2904  23220  296.0 700.0                  Andersen et al. (2009)"
    """
    HEADER = dict(
        Molecule = {
            "ChemicalFormula": header_line[0:20].strip(), 
            "CommonName": None
        },
        numin = float(header_line[20:30].strip()),
        numax = float(header_line[30:40].strip()),
        npnts = int(header_line[40:47].strip()),
        Temperature = float(header_line[47:54].strip()),
        Pressure = float(header_line[54:60].strip()),
        ReferenceAlias = header_line[60:100].strip()
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

def parse_header_multi(header_line): # for the multi-header file, accounts for the main objects 
    """
    Parse cross section header given in semi-HIRTAN format.
    EXAMPLE:
    "               SO2F2  599.9980 1999.2904  23220  296.0 700.0                  Andersen et al. (2009)"
    """
    HEADER = dict(
        molecule_aliases = [header_line[0:20].strip(), header_line[75:90].strip()],
        numin = float(header_line[20:30].strip()),
        numax = float(header_line[30:40].strip()),
        npnts = int(header_line[40:47].strip()),
        temperature = float(header_line[47:54].strip()),
        pressure = float(header_line[54:60].strip()),
        broadener = header_line[90:97].strip(),
        sigma_max = float(header_line[60:70].strip()),
        local_ref_id = int(header_line[97:100].strip()),
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
        HEADER = parse_header_multi(header_line)
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
