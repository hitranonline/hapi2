# -*- coding: utf-8 -*-

from multiprocessing import Process
from numba import njit, prange, jit
import numpy as np
import numba # for explicit parameter declaration
#from hapi import *
import hapi as h
#import hapi_TEST as h # geek version of HAPI to debug the comparisons
from .sdv import PROFILE_SDVOIGT,PROFILE_LORENTZ,PROFILE_DOPPLER
from time import time

from .settings import FASTMATH
from .settings import PARALLEL

from hapi import PYTIPS,DefaultIntensityThreshold,DefaultOmegaWingHW,\
                 listOfTuples,getDefaultValuesForXsect
from numpy import sort as npsort

#PROFILE = PROFILE_VOIGT # HAPI version
PROFILE = PROFILE_SDVOIGT # numba version

# In this "robust" version of arange the grid doesn't suffer 
# from the shift of the nodes due to error accumulation.
# This effect is pronounced only if the step is sufficiently small.
#@njit([numba.float64[:](numba.float64,numba.float64,numba.float64)],parallel=PARALLEL)
@njit
def arange_(lower,upper,step):
    npnt = np.floor((upper-lower)/step)+1
    npnt = np.int32(npnt) # strong typing is needed for Numba
    upper_new = lower + step*(npnt-1)
    if np.abs((upper-upper_new)-step) < 1e-10:
        upper_new += step
        npnt += 1    
    return np.linspace(lower,upper_new,npnt)
    
"""
=================================
ISOTOPOLOGUE-SPECIFIC PARAMETERS
=================================

M,I - HITRAN molecule and isotopologue numbers

MOLECULE_INDEX = [1,2,3,4,......N_mols] -> contains offsets for different molecules
ISOTOPOLOGUE_INDEX = [1,2,3,4,   1,2,3,4,5,   1,2,3,4,5,6,7,8,   1,2,3,   1,2,  ...]
                      mol_1       mol_2       mol_3              mol_4    mol_5
                      
LIST OF ISOTOPOLOGUE-SPECIFIC PARAMETERS: 
    1) HITRAN molecule id => POSITION 1
    2) HITRAN isotopologue id => POSITION 2
    1) abundance (default/natural or currently specified) => POSITION 3
    2) partition sums (at reference and current temperature) => POSITIONS 4 AND 5
    3) molecular (isotopologic) mass (needed for producing Doppler broadening parameters) => POSITION 6                    
"""

"""
===================
SCHEME OF ISO_INDEX
===================

[  M   I    abun_nat       abun     Q_Tref     Q_T     mass  ]   
   0   1       2             3        4         5        6
    
HOW TO RETRIEVE THE NEEDED LINE:

    
    
"""

"""
GENERATE INDEX FOR ISOTOPOLOGUE-SPECIFIC PARAMETERS

#        M    I             id    iso_name                    abundance           mass        mol_name

ISO = {
                            
(        1,   1    ): [      1,  'H2(16O)',                   0.997317,           18.010565,      'H2O'     ],
"""

def generate_indexes(ISO):
    """
    Generate index for molecule- and isotopologue-specific parameters.
    Partition sums are not calculated because it is too computationally expensive.
    """
    
    # Get all needed fields from HAPI ISO dictionary and make dictionary
    ISO_INDEX = []
    
    for M,I in ISO:
        abun_nat = ISO[(M,I)][2]
        abun = -1
        Q_Tref = 0.0
        Q_T = 0.0
        mass = ISO[(M,I)][3]
        INDEX_LINE = [M,I,abun_nat,abun,Q_Tref,Q_T,mass]
        ISO_INDEX.append(INDEX_LINE)

    # Sort index by M,I
    ISO_INDEX = sorted(ISO_INDEX)
    
    # Create a molecule index
    MOL_INDEX = np.full(ISO_INDEX[-1][0]+1,fill_value=-1,dtype=np.int)
    for cnt,INDEX_LINE in enumerate(ISO_INDEX):
        M = INDEX_LINE[0]
        if MOL_INDEX[M]==-1: MOL_INDEX[M] = cnt
    
    # Convert iso index to numpy array
    ISO_INDEX = np.array(ISO_INDEX)
    
    return ISO_INDEX,MOL_INDEX

# Generate default indexes for isotopologues and molecules
ISO_INDEX_DEFAULT,MOL_INDEX = generate_indexes(h.ISO)
        
@njit
def get_iso_index_line(M,I,MOL_INDEX,ISO_INDEX):
    offset = MOL_INDEX[M]
    ind = offset+I-1 # VERY ERROR-PRONE IF ISO_IDS DONT OBEY THE STRICT ORDER!!!
    return ISO_INDEX[ind,:]
        
# ============================
# CALCULATE DOPPLER BROADENING
# ============================

#@njit(parallel=PARALLEL)
@njit
def calculate_GammaD(N,T,LineCenterDB,MoleculeNumberDB,IsoNumberDB,ISO_INDEX,MOL_INDEX):
    """
    Calculate Doppler broadening parameter (gamma_d)
    from temperature (T), isotopologue mass (m) and line center (nu)
    """
    GammaD = np.zeros(N)
    cMassMol = 1.66053873e-27 # internal constant given in HAPI code
    #for i in prange(N):
    for i in range(N):
        # ATTENTION!!! IsoNumberDB parameter can be ambiguous (e.g. for CO2)
        m = get_iso_index_line(MoleculeNumberDB[i],IsoNumberDB[i],MOL_INDEX,ISO_INDEX)[6] * cMassMol * 1000
        GammaD[i] = np.sqrt(2*h.cBolts*T*np.log(2)/m/h.cc**2)*LineCenterDB[i]
    return GammaD

# ===============================================
# APPLY WEIGHTS ACCORDING PARTIAL PRESSURES/VMRS
# ===============================================

#@njit(parallel=PARALLEL)
@njit
def weighted_sum(N,M,PARS,MIX):
    """
    Generic function to aply the mixture-dependence for some of the parameters
    such as broadening and shifting coefficients.
    INPUT PARAMETERS: 
    PARS: 2D-array consisting of columns; i-th column contains parameter values
          for i-th component of the mixture; array dimension is NxM.
    MIX: 1D array of values; i-th is a VMR of i-th component of the mixture.
          Array dimension is M.
    """
    RES = np.zeros(N)
    #for i in prange(M):
    for i in range(M):
        RES += M[i]*PARS[:,i]
    return RES

# ====================================================================
# GET ISOS MATRIX (DESCRPTION OF THE RADIATIVELY ACTIVE ISOTOPOLOGUES)    
# ====================================================================
def GET_ISOS_DEFAULT_ABUN(N,MOLEC_ID,LOCAL_ISO_ID):
    """
    Get the description of the radiatively active compounds of the modeled mixture.
    The abundances are set to -1 (default values)
    """
    # https://docs.scipy.org/doc/numpy-1.13.0/reference/generated/numpy.full.html
    ABUN = np.full(N,fill_value=-1.,dtype=np.float64)
    # https://stackoverflow.com/questions/44409084/how-to-zip-two-1d-numpy-array-to-2d-numpy-array
    ZIPPED = np.array(list(zip(MOLEC_ID,LOCAL_ISO_ID,ABUN)))
    # https://stackoverflow.com/questions/16970982/find-unique-rows-in-numpy-array
    ISOS = np.unique(ZIPPED,axis=0)
    return ISOS
    
#========================================================================
# ENV_DEPENEDENCE_<PARNAME> SHOULD ACCEPT THE PARAMETERS AS AN ARRAY/LIST
#========================================================================

#def calculate_psums(T,Tref,ISO_INDEX,MOL_INDEX,ISOS,partsum=None):
#    """
#    Calculate partition sums for isotopologues specified in Numpy array ISOS.
#    This array is a matrix with two columns: 0->molecule IDs, 1->isotopologue IDs.
#    """
#    if not partsum:
#        partsum = h.PYTIPS
#    for i in enumerate(ISOS):
#        ISO = ISOS[i,:]
#        M = ISO[0]; I = ISO[1]
#        offset = MOL_INDEX[M]
#        # calculate Q at Tref
#        ISO_INDEX[offset+IsoNumberDB-1][4] = partsum(MoleculeNumberDB,IsoNumberDB,Tref)
#        # calculate Q at T
#        ISO_INDEX[offset+IsoNumberDB-1][5] = partsum(MoleculeNumberDB,IsoNumberDB,T)

#@jit  # ENABLING JIT MAKES ALL CODE RUN SLOWER!!!
def ENV_DEPENDENCE_ISO_INDEX(ISO_INDEX,ISOS,T,Tref,partsum):
    """
    Calculate some environment-dependent parameter in ISO_INDEX so they
    satisfy the conditions:
    1) Abundances (depending on the mixture)
    2) Partition sums (depending on the current and reference temperature)
    """
    #for i in prange(ISOS.shape[0]):
    for i in range(ISOS.shape[0]):
        ISO = ISOS[i,:]
        M = ISO[0]; I = ISO[1]; abun = ISO[2]   
        M = int(M); I = int(I)
        # find index of the current isotopologue
        ISO_INDEX_LINE = get_iso_index_line(M,I,MOL_INDEX,ISO_INDEX)
        # calculate Q at Tref
        ISO_INDEX_LINE[4] = partsum(M,I,Tref)
        # calculate Q at T
        ISO_INDEX_LINE[5] = partsum(M,I,T)
        # sort things out with abundances
        if abun>0: 
            ISO_INDEX[ind][3] = abun
    
#n_lines,SW,T,Tref,E_LOWER,NU,ISOS,MOLEC_ID,LOCAL_ISO_ID,ISO_INDEX,MOL_INDEX
#NLINES,SW,T,Tref,E_LOWER,NU,ISOS,MOLEC_ID,LOCAL_ISO_ID,ISO_INDEX,MOL_INDEX
#@njit(parallel=PARALLEL)
@njit
def ENV_DEPENDENCE_SW(N,SW,T,Tref,ELOWER,NU,
                      ISOS,MOLEC_ID,LOCAL_ISO_ID,ISO_INDEX,MOL_INDEX):
    """
    Environment dependence for intensity parameter.
    Input scalars: N, T, Tref
    The following input arrays are used: 
       NAME               DIM     COMMENT
       =============================================
       LineIntensity      N       OUTPUT
       SW                 N       S
       E_LOWER            N       elower
       NU                 N       nu
       Isotopologues      N       local_iso_id
       MOLEC_ID           N       molec_id
       LOCAL_ISO_ID       N       local_iso_id
    """
    const = 1.4388028496642257
    LineIntensity = np.zeros(N)
    #for i in prange(N):
    for i in range(N):
        M = MOLEC_ID[i]; I = LOCAL_ISO_ID[i]
        ISO_INDEX_LINE = get_iso_index_line(M,I,MOL_INDEX,ISO_INDEX)
        SigmaTref = ISO_INDEX_LINE[4]
        SigmaT = ISO_INDEX_LINE[5]
        ch = np.exp(-const*ELOWER[i]/T)*(1-np.exp(-const*NU[i]/T))
        zn = np.exp(-const*ELOWER[i]/Tref)*(1-np.exp(-const*NU[i]/Tref))
        LineIntensity[i] = SW[i]*SigmaTref/SigmaT*ch/zn
    return LineIntensity

#@njit(parallel=PARALLEL)
@njit
def ENV_DEPENDENCE_GAMMA0(N,Gamma0_ref,T,Tref,p,pref,TempRatioPower):
    """
    Environment dependence for pressure broadening parameter.
    Input scalars: N, ,T, Tref, p, pref
    The following input arrays are used: 
       NAME               DIM     COMMENT
       =============================================
       Gamma0             N       OUTPUT
       Gamma0_ref         N       gamma_<agent>
       TempRatioPower     N       n_<agent>
    """
    Gamma0 = np.zeros(N)
    #for i in prange(N):
    for i in range(N):
        Gamma0[i] = Gamma0_ref[i]*p/pref*(Tref/T)**TempRatioPower[i]
    return Gamma0
        
#@njit(parallel=PARALLEL)
@njit
def ENV_DEPENDENCE_DELTA0(N,Delta0_ref,p,pref):
    """
    Environment dependence for pressure shifting parameter.
    Input scalars: N, p, pref
    The following input arrays are used: 
       NAME               DIM     COMMENT
       =============================================
       Delta0             N       OUTPUT
       Delta0_ref         N       delta_<agent>
    """
    Delta0 = np.zeros(N)
    #for i in prange(N):
    for i in range(N):
        Delta0[i] = Delta0_ref[i]*p/pref
    return Delta0
    
#======================================================================
# INTERFACE FOR BACKWARDS COMPATIBILITY WITH HAPI v1.0 (VOIGT PROFILE)
#======================================================================
def absorptionCoefficient_Voigt(Components=None,SourceTables=None,partitionFunction=PYTIPS,
                                Environment=None,OmegaRange=None,OmegaStep=None,OmegaWing=None,
                                IntensityThreshold=DefaultIntensityThreshold,
                                OmegaWingHW=DefaultOmegaWingHW,
                                GammaL='gamma_air', HITRAN_units=True, LineShift=True,
                                File=None, Format=None, OmegaGrid=None,
                                WavenumberRange=None,WavenumberStep=None,WavenumberWing=None,
                                WavenumberWingHW=None,WavenumberGrid=None,
                                Diluent={},EnvDependences=None,NCORES=1, TDoppler=None):
    """
    ======================================================================
    FAST NUMBA IMPLEMENTATION OF THE ABSORPTION CROSS-SECTION CALCULATION
    ======================================================================
    INPUT PARAMETERS: 
        Components:  list of tuples [(M,I,D)], where
                        M - HITRAN molecule number,
                        I - HITRAN isotopologue number,
                        D - relative abundance (optional)
        SourceTables:  list of tables from which to calculate cross-section   (optional)
        partitionFunction:  pointer to partition function (default is PYTIPS) (optional)
        Environment:  dictionary containing thermodynamic parameters.
                        'p' - pressure in atmospheres,
                        'T' - temperature in Kelvin
                        Default={'p':1.,'T':296.}
        WavenumberRange:  wavenumber range to consider.
        WavenumberStep:   wavenumber step to consider. 
        WavenumberWing:   absolute wing for calculating a lineshape (in cm-1) 
        WavenumberWingHW:  relative wing for calculating a lineshape (in halfwidths)
        IntensityThreshold:  threshold for intensities
        GammaL:  specifies broadening parameter ('gamma_air' or 'gamma_self')
        HITRAN_units:  use cm2/molecule (True) or cm-1 (False) for absorption coefficient
        File:   write output to file (if specified)
        Format:  c-format of file output (accounts for significant digits in WavenumberStep)
    OUTPUT PARAMETERS: 
        Wavenum: wavenumber grid with respect to parameters WavenumberRange and WavenumberStep
        Xsect: absorption coefficient calculated on the grid
    ---
    DESCRIPTION:
        Calculate absorption coefficient using Voigt profile.
        Absorption coefficient is calculated at arbitrary temperature and pressure.
        User can vary a wide range of parameters to control a process of calculation.
        The choise of these parameters depends on properties of a particular linelist.
        Default values are a sort of guess which gives a decent precision (on average) 
        for a reasonable amount of cpu time. To increase calculation accuracy,
        user should use a trial and error method.
    ---
    EXAMPLE OF USAGE:
        nu,coef = absorptionCoefficient_Voigt(((2,1),),'co2',WavenumberStep=0.01,
                                              HITRAN_units=False,GammaL='gamma_self')
    ---
    """
    t = time()
    
    # raise exceptions for not implemented features
    if EnvDependences is not None: raise NotImplementedError('custom environment dependences are not implemented for the Numba version')
    if LineShift is not True: raise NotImplementedError('disabling line shift is not implemented for the Numba version')
    if File is not None: raise NotImplementedError('saving to file is not implemented for the Numba version')
    if Format is not None: raise NotImplementedError('saving to file is not implemented for the Numba version')
    #if Components is not None: raise NotImplementedError('filtering by components is not implemented for the Numba version')
    
    # Paremeters OmegaRange,OmegaStep,OmegaWing,OmegaWingHW, and OmegaGrid
    # are deprecated and given for backward compatibility with the older versions.
    if WavenumberRange is not None:  OmegaRange=WavenumberRange
    if WavenumberStep is not None:   OmegaStep=WavenumberStep
    if WavenumberWing is not None:   OmegaWing=WavenumberWing
    if WavenumberWingHW is not None: OmegaWingHW=WavenumberWingHW
    if WavenumberGrid is not None:   OmegaGrid=WavenumberGrid

    # "bug" with 1-element list
    Components = listOfTuples(Components)
    SourceTables = listOfTuples(SourceTables)
    
    # raise exceptions for not implemented features
    if len(SourceTables)>1: raise NotImplementedError('handling more than one table is not implemented for the Numba version')
    
    # Hack to disregard the components parameter
    Components = -1
    
    # determine final input values 
    Components,SourceTables,Environment,OmegaRange,OmegaStep,OmegaWing,\
    IntensityThreshold,Format = \
       getDefaultValuesForXsect(Components,SourceTables,Environment,OmegaRange,
                                OmegaStep,OmegaWing,IntensityThreshold,Format)
    
    # warn user about too large omega step
    if OmegaStep>0.1: warn('Big wavenumber step: possible accuracy decline')
        
    # setup the Diluent variable
    GammaL = GammaL.lower()
    if not Diluent:
        if GammaL == 'gamma_air':
            Diluent = {'air':1.}
        elif GammaL == 'gamma_self':
            Diluent = {'self':1.}
        else:
            raise Exception('Unknown GammaL value: %s' % GammaL)
        
    # Simple check
    #print(Diluent)  # Added print statement # CHANGED RJH 23MAR18  # Simple check
    for key in Diluent:
        val = Diluent[key]
        if val < 0 or val > 1: # if val < 0 and val > 1:# CHANGED RJH 23MAR18
            raise Exception('Diluent fraction must be in [0,1]')
                
                
                
    #print('\n  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    #print('  ~~ NUMBA XSC CALCULATION ~~~~~~~~~~~~~~~~~~')
    #print('  ~~ Using %d core(s)'%(NCORES) )
    #print('  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    #print('  ~~ Grid Points : ~%d'%((OmegaRange[1]-OmegaRange[0])/OmegaStep))
    #print('  ~~ Temperature : %.2f'%(Environment['T']) )
    #if TDoppler:
    #    print('  ~~ Doppler T   : %F'%TDoppler)
    #print('  ~~ Pressure    : %.4f'%(Environment['p']) )
    #print('  ~~ OmegaRange  : %s-%s cm-1'%(OmegaRange[0],OmegaRange[1]))
    #print('  ~~ OmegaStep   : %.4f'%(OmegaStep))
    #if OmegaWingHW:
    #    print('  ~~ OmegaWingHW : %.2f'%(OmegaWingHW))
    #print('  ~~ OmegaWing   : %.2f'%(OmegaWing))
    #print('  ~~ Diluent     : air:%.2f self:%.2f '%(Diluent.get('air',0),Diluent.get('self',0)))
    
    # New code starts here
    TABLE_NAME = SourceTables[0]
    MOLEC_ID,LOCAL_ISO_ID = h.getColumns(TABLE_NAME,['molec_id','local_iso_id'])
    NLINES = len(MOLEC_ID)
    ISOS = GET_ISOS_DEFAULT_ABUN(NLINES,MOLEC_ID,LOCAL_ISO_ID)
    DILUENT = list(Diluent.items())    
    OmegaRange = np.array(OmegaRange)
    
    #print('  ~~ %f seconds elapsed for pre-calc'%(time()-t))
    #print('  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    
    t = time()    

    nu,xsc = ABSCOEF_FAST(NLINES,TABLE_NAME,ISOS,DILUENT,
                 Omegas=WavenumberGrid,
                 OmegaWing=OmegaWing,OmegaWingHW=OmegaWingHW,reflect=False,
                 T=Environment['T'],Tref=296.0, TDoppler=TDoppler,
                 p=Environment['p'],pref=1.0,
                 partsum=PYTIPS,
                 profile=1,
                 test=False,
                 NCORES=NCORES)
                 
    #print('  ~~ %f seconds elapsed for calc'%(time()-t))
    #print('  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

    return nu,xsc

def absorptionCoefficient_Generic(profile,calcpars,*args,**kwargs):
    return absorptionCoefficient_Voigt(*args,**kwargs)

#========================================================================
# INTERFACE FOR BACKWARDS COMPATIBILITY WITH HAPI v1.0 (LORENTZ PROFILE)
#========================================================================
def absorptionCoefficient_Lorentz(Components=None,SourceTables=None,partitionFunction=PYTIPS,
                                Environment=None,OmegaRange=None,OmegaStep=None,OmegaWing=None,
                                IntensityThreshold=DefaultIntensityThreshold,
                                OmegaWingHW=DefaultOmegaWingHW,
                                GammaL='gamma_air', HITRAN_units=True, LineShift=True,
                                File=None, Format=None, OmegaGrid=None,
                                WavenumberRange=None,WavenumberStep=None,WavenumberWing=None,
                                WavenumberWingHW=None,WavenumberGrid=None,
                                Diluent={},EnvDependences=None,NCORES=1):
    """
    ======================================================================
    FAST NUMBA IMPLEMENTATION OF THE ABSORPTION CROSS-SECTION CALCULATION
    ======================================================================
    INPUT PARAMETERS: 
        Components:  list of tuples [(M,I,D)], where
                        M - HITRAN molecule number,
                        I - HITRAN isotopologue number,
                        D - relative abundance (optional)
        SourceTables:  list of tables from which to calculate cross-section   (optional)
        partitionFunction:  pointer to partition function (default is PYTIPS) (optional)
        Environment:  dictionary containing thermodynamic parameters.
                        'p' - pressure in atmospheres,
                        'T' - temperature in Kelvin
                        Default={'p':1.,'T':296.}
        WavenumberRange:  wavenumber range to consider.
        WavenumberStep:   wavenumber step to consider. 
        WavenumberWing:   absolute wing for calculating a lineshape (in cm-1) 
        WavenumberWingHW:  relative wing for calculating a lineshape (in halfwidths)
        IntensityThreshold:  threshold for intensities
        GammaL:  specifies broadening parameter ('gamma_air' or 'gamma_self')
        HITRAN_units:  use cm2/molecule (True) or cm-1 (False) for absorption coefficient
        File:   write output to file (if specified)
        Format:  c-format of file output (accounts for significant digits in WavenumberStep)
    OUTPUT PARAMETERS: 
        Wavenum: wavenumber grid with respect to parameters WavenumberRange and WavenumberStep
        Xsect: absorption coefficient calculated on the grid
    ---
    DESCRIPTION:
        Calculate absorption coefficient using Voigt profile.
        Absorption coefficient is calculated at arbitrary temperature and pressure.
        User can vary a wide range of parameters to control a process of calculation.
        The choise of these parameters depends on properties of a particular linelist.
        Default values are a sort of guess which gives a decent precision (on average) 
        for a reasonable amount of cpu time. To increase calculation accuracy,
        user should use a trial and error method.
    ---
    EXAMPLE OF USAGE:
        nu,coef = absorptionCoefficient_Voigt(((2,1),),'co2',WavenumberStep=0.01,
                                              HITRAN_units=False,GammaL='gamma_self')
    ---
    """
    t = time()
    
    # raise exceptions for not implemented features
    if EnvDependences is not None: raise NotImplementedError('custom environment dependences are not implemented for the Numba version')
    if LineShift is not True: raise NotImplementedError('disabling line shift is not implemented for the Numba version')
    if File is not None: raise NotImplementedError('saving to file is not implemented for the Numba version')
    if Format is not None: raise NotImplementedError('saving to file is not implemented for the Numba version')
    #if Components is not None: raise NotImplementedError('filtering by components is not implemented for the Numba version')
    
    # Paremeters OmegaRange,OmegaStep,OmegaWing,OmegaWingHW, and OmegaGrid
    # are deprecated and given for backward compatibility with the older versions.
    if WavenumberRange is not None:  OmegaRange=WavenumberRange
    if WavenumberStep is not None:   OmegaStep=WavenumberStep
    if WavenumberWing is not None:   OmegaWing=WavenumberWing
    if WavenumberWingHW is not None: OmegaWingHW=WavenumberWingHW
    if WavenumberGrid is not None:   OmegaGrid=WavenumberGrid

    # "bug" with 1-element list
    Components = listOfTuples(Components)
    SourceTables = listOfTuples(SourceTables)
    
    # raise exceptions for not implemented features
    if len(SourceTables)>1: raise NotImplementedError('handling more than one table is not implemented for the Numba version')
    
    # Hack to disregard the components parameter
    Components = -1
    
    # determine final input values 
    Components,SourceTables,Environment,OmegaRange,OmegaStep,OmegaWing,\
    IntensityThreshold,Format = \
       getDefaultValuesForXsect(Components,SourceTables,Environment,OmegaRange,
                                OmegaStep,OmegaWing,IntensityThreshold,Format)
    
    # warn user about too large omega step
    if OmegaStep>0.1: warn('Big wavenumber step: possible accuracy decline')
        
    # setup the Diluent variable
    GammaL = GammaL.lower()
    if not Diluent:
        if GammaL == 'gamma_air':
            Diluent = {'air':1.}
        elif GammaL == 'gamma_self':
            Diluent = {'self':1.}
        else:
            raise Exception('Unknown GammaL value: %s' % GammaL)
        
    # Simple check
    print(Diluent)  # Added print statement # CHANGED RJH 23MAR18  # Simple check
    for key in Diluent:
        val = Diluent[key]
        if val < 0 or val > 1: # if val < 0 and val > 1:# CHANGED RJH 23MAR18
            raise Exception('Diluent fraction must be in [0,1]')
                
    # New code starts here
    TABLE_NAME = SourceTables[0]
    MOLEC_ID,LOCAL_ISO_ID = h.getColumns(TABLE_NAME,['molec_id','local_iso_id'])
    NLINES = len(MOLEC_ID)
    ISOS = GET_ISOS_DEFAULT_ABUN(NLINES,MOLEC_ID,LOCAL_ISO_ID)
    DILUENT = list(Diluent.items())    
    OmegaRange = np.array(OmegaRange)
    
    #print('  ~~ %f seconds elapsed for pre-calc'%(time()-t))
    
    t = time()    

    nu,xsc = ABSCOEF_FAST(NLINES,TABLE_NAME,ISOS,DILUENT,
                 Omegas=WavenumberGrid,
                 OmegaWing=OmegaWing,OmegaWingHW=OmegaWingHW,reflect=False,
                 T=Environment['T'],Tref=296.0,
                 p=Environment['p'],pref=1.0,
                 partsum=PYTIPS,
                 profile=2,
                 test=False,
                 NCORES=NCORES)
                 
    #print('  ~~ %f seconds elapsed for calc'%(time()-t))

    return nu,xsc

#========================================================================
# INTERFACE FOR BACKWARDS COMPATIBILITY WITH HAPI v1.0 (DOPPLER PROFILE)
#========================================================================
def absorptionCoefficient_Doppler(Components=None,SourceTables=None,partitionFunction=PYTIPS,
                                Environment=None,OmegaRange=None,OmegaStep=None,OmegaWing=None,
                                IntensityThreshold=DefaultIntensityThreshold,
                                OmegaWingHW=DefaultOmegaWingHW,
                                GammaL='gamma_air', HITRAN_units=True, LineShift=True,
                                File=None, Format=None, OmegaGrid=None,
                                WavenumberRange=None,WavenumberStep=None,WavenumberWing=None,
                                WavenumberWingHW=None,WavenumberGrid=None,
                                Diluent={},EnvDependences=None,NCORES=1, TDoppler=None):
    """
    ======================================================================
    FAST NUMBA IMPLEMENTATION OF THE ABSORPTION CROSS-SECTION CALCULATION
    ======================================================================
    INPUT PARAMETERS: 
        Components:  list of tuples [(M,I,D)], where
                        M - HITRAN molecule number,
                        I - HITRAN isotopologue number,
                        D - relative abundance (optional)
        SourceTables:  list of tables from which to calculate cross-section   (optional)
        partitionFunction:  pointer to partition function (default is PYTIPS) (optional)
        Environment:  dictionary containing thermodynamic parameters.
                        'p' - pressure in atmospheres,
                        'T' - temperature in Kelvin
                        Default={'p':1.,'T':296.}
        WavenumberRange:  wavenumber range to consider.
        WavenumberStep:   wavenumber step to consider. 
        WavenumberWing:   absolute wing for calculating a lineshape (in cm-1) 
        WavenumberWingHW:  relative wing for calculating a lineshape (in halfwidths)
        IntensityThreshold:  threshold for intensities
        GammaL:  specifies broadening parameter ('gamma_air' or 'gamma_self')
        HITRAN_units:  use cm2/molecule (True) or cm-1 (False) for absorption coefficient
        File:   write output to file (if specified)
        Format:  c-format of file output (accounts for significant digits in WavenumberStep)
    OUTPUT PARAMETERS: 
        Wavenum: wavenumber grid with respect to parameters WavenumberRange and WavenumberStep
        Xsect: absorption coefficient calculated on the grid
    ---
    DESCRIPTION:
        Calculate absorption coefficient using Voigt profile.
        Absorption coefficient is calculated at arbitrary temperature and pressure.
        User can vary a wide range of parameters to control a process of calculation.
        The choise of these parameters depends on properties of a particular linelist.
        Default values are a sort of guess which gives a decent precision (on average) 
        for a reasonable amount of cpu time. To increase calculation accuracy,
        user should use a trial and error method.
    ---
    EXAMPLE OF USAGE:
        nu,coef = absorptionCoefficient_Voigt(((2,1),),'co2',WavenumberStep=0.01,
                                              HITRAN_units=False,GammaL='gamma_self')
    ---
    """
    t = time()
    
    # raise exceptions for not implemented features
    if EnvDependences is not None: raise NotImplementedError('custom environment dependences are not implemented for the Numba version')
    if LineShift is not True: raise NotImplementedError('disabling line shift is not implemented for the Numba version')
    if File is not None: raise NotImplementedError('saving to file is not implemented for the Numba version')
    if Format is not None: raise NotImplementedError('saving to file is not implemented for the Numba version')
    #if Components is not None: raise NotImplementedError('filtering by components is not implemented for the Numba version')
    
    # Paremeters OmegaRange,OmegaStep,OmegaWing,OmegaWingHW, and OmegaGrid
    # are deprecated and given for backward compatibility with the older versions.
    if WavenumberRange is not None:  OmegaRange=WavenumberRange
    if WavenumberStep is not None:   OmegaStep=WavenumberStep
    if WavenumberWing is not None:   OmegaWing=WavenumberWing
    if WavenumberWingHW is not None: OmegaWingHW=WavenumberWingHW
    if WavenumberGrid is not None:   OmegaGrid=WavenumberGrid

    # "bug" with 1-element list
    Components = listOfTuples(Components)
    SourceTables = listOfTuples(SourceTables)
    
    # raise exceptions for not implemented features
    if len(SourceTables)>1: raise NotImplementedError('handling more than one table is not implemented for the Numba version')
    
    # Hack to disregard the components parameter
    Components = -1
    
    # determine final input values 
    Components,SourceTables,Environment,OmegaRange,OmegaStep,OmegaWing,\
    IntensityThreshold,Format = \
       getDefaultValuesForXsect(Components,SourceTables,Environment,OmegaRange,
                                OmegaStep,OmegaWing,IntensityThreshold,Format)
    
    # warn user about too large omega step
    if OmegaStep>0.1: warn('Big wavenumber step: possible accuracy decline')
        
    # setup the Diluent variable
    GammaL = GammaL.lower()
    if not Diluent:
        if GammaL == 'gamma_air':
            Diluent = {'air':1.}
        elif GammaL == 'gamma_self':
            Diluent = {'self':1.}
        else:
            raise Exception('Unknown GammaL value: %s' % GammaL)
        
    # Simple check
    print(Diluent)  # Added print statement # CHANGED RJH 23MAR18  # Simple check
    for key in Diluent:
        val = Diluent[key]
        if val < 0 or val > 1: # if val < 0 and val > 1:# CHANGED RJH 23MAR18
            raise Exception('Diluent fraction must be in [0,1]')
                
    # New code starts here
    TABLE_NAME = SourceTables[0]
    MOLEC_ID,LOCAL_ISO_ID = h.getColumns(TABLE_NAME,['molec_id','local_iso_id'])
    NLINES = len(MOLEC_ID)
    ISOS = GET_ISOS_DEFAULT_ABUN(NLINES,MOLEC_ID,LOCAL_ISO_ID)
    DILUENT = list(Diluent.items())    
    OmegaRange = np.array(OmegaRange) # remove?
    
    #print('  ~~ %f seconds elapsed for pre-calc'%(time()-t))
    
    t = time()    

    nu,xsc = ABSCOEF_FAST(NLINES,TABLE_NAME,ISOS,DILUENT,
                 Omegas=WavenumberGrid,
                 OmegaWing=OmegaWing,OmegaWingHW=OmegaWingHW,reflect=False,
                 T=Environment['T'],Tref=296.0, TDoppler=None,
                 p=Environment['p'],pref=1.0,
                 partsum=PYTIPS,
                 profile=3,
                 test=False,
                 NCORES=NCORES)
                 
    #print('  ~~ %f seconds elapsed for calc'%(time()-t))

    return nu,xsc
    
#================================================================================
# PYTHON WRAPPER FOR THE FAST FUNCTION FOR CALCULATING THE ABSORPTION COEFFICIENT
#================================================================================

# OLD VERSION
#def ABSCOEF_FAST(TABLE_NAME,OmegaRange=None,OmegaStep=None,OmegaWing=None,OmegaWingHW=None,reflect=True):
#
#    NLINES = h.LOCAL_TABLE_CACHE[TABLE_NAME]['header']['number_of_rows']
#    NU = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['nu']
#    SW = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['sw']
#    ELOWER = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['elower']
#    MOLEC_ID = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['molec_id']
#    LOCAL_ISO_ID = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['local_iso_id']
#    GAMMA_AIR = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['gamma_air']
#    DELTA_AIR = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['delta_air']
#    
#    DATA = np.array(list(zip(NU,SW,ELOWER,MOLEC_ID,LOCAL_ISO_ID,GAMMA_AIR,DELTA_AIR))) # in Python 3 zip is an iterator !!!
#    
#    cMassMol = 1.66053873e-27 # hapi
#        
#    return CALC_(DATA,NLINES,OmegaRange,OmegaStep,OmegaWing,OmegaWingHW,reflect)
     
# NEW VERSION
#@jit  # ENABLING JIT MAKES ALL CODE RUN ~TWO TIMES SLOWER!!!
def ABSCOEF_FAST(NLINES,TABLE_NAME,ISOS,DILUENT,
                 Omegas,
                 OmegaWing=None,OmegaWingHW=None,reflect=True,
                 T=296.0,Tref=296.0, TDoppler=None,
                 p=1.0,pref=1.0,
                 partsum=h.PYTIPS,
                 profile=1,
                 test=False,
                 NCORES=1):
    """
    ==================
    INPUT PARAMETERS:
    ==================
    ISOS: Matrix, with each line representing particular isotopologue
          in mixture. Line consists of M, I, abun; 
          Negative abundance value means using default (natural abundance)
          Zero abundance value means skip this isotopologue
          Isotopologues not mentioned in ISOS will be ignored!!!
    DILUENT: List of tuples containing broadening agents:
             E.g.: [('air',0.3),('self',0.7)]
    """
    
    # Do some type conversions for safety reasons (except for the arrays from HAPI)
    NLINES = np.int64(NLINES)
    OmegaWing = np.float64(OmegaWing)
    OmegaWingHW = np.float64(OmegaWingHW)

    t = time()
    
    # Profile selection
    #if profile==1:
    #    print('  ~~ Using VOIGT profile')
    #elif profile==2:
    #    print('  ~~ Using LORENTZ profile')
    #elif profile==3:
    #    print('  ~~ Using DOPPLER profile')
    #else:
    #    raise Excepion('Unknown profile number: %d'%profile)
        
    # Get molecule and isotopologue local HITRAN ids
    MOLEC_ID = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['molec_id']
    LOCAL_ISO_ID = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['local_iso_id']
        
    # Get centers, intensities, and lower states
    NU = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['nu']
    SW = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['sw']
    ELOWER = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['elower']

    # Calculate additional parameters in ISO_INDEX: abundances (depend on isotopic constitution)
    # and partition sums (depend on PS routine, reference temperatures and current mixture temperature)
    ISO_INDEX = ISO_INDEX_DEFAULT.copy() # copy iso index in order to avoid side effects
    ENV_DEPENDENCE_ISO_INDEX(ISO_INDEX,ISOS,T,Tref,partsum) # go through ISO_INDEX  and change partition sums and current abundances
    
    # Get intensities and account for T-dependences and isotopic abundances    
    SW = ENV_DEPENDENCE_SW(NLINES,SW,T,Tref,ELOWER,NU,ISOS,MOLEC_ID,LOCAL_ISO_ID,ISO_INDEX,MOL_INDEX)
    
    # Calculate Doppler broadening
    if TDoppler:
        TDoppler = np.float64(TDoppler)
        GAMMA_D = calculate_GammaD(NLINES,TDoppler,NU,MOLEC_ID,LOCAL_ISO_ID,ISO_INDEX,MOL_INDEX)
    else:
        GAMMA_D = calculate_GammaD(NLINES,T,NU,MOLEC_ID,LOCAL_ISO_ID,ISO_INDEX,MOL_INDEX)
    
    # Get Lorentzian broadening parameters and account for their T- and p-dependences
    GAMMA_L = np.zeros(NLINES)
    for broadener,fraction in DILUENT:
        GAMMA_BR = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['gamma_%s'%broadener]
        if broadener=='self': # !!! THIS SHOULD BE REDONE !!!
            N_BR = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['n_air']
        else:
            N_BR = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['n_%s'%broadener.lower()]
        GAMMA_BR = ENV_DEPENDENCE_GAMMA0(NLINES,GAMMA_BR,T,Tref,p,pref,N_BR)
        GAMMA_L += GAMMA_BR*fraction
        
    # Get shifting parameters and account for their T- and p-dependences
    DELTA = np.zeros(NLINES)
    for broadener,fraction in DILUENT:
        if broadener=='self': # !!! THIS SHOULD BE REDONE !!!
            DELTA_BR = np.zeros(NLINES)
        else:
            DELTA_BR = h.LOCAL_TABLE_CACHE[TABLE_NAME]['data']['delta_%s'%broadener]
        DELTA_BR = ENV_DEPENDENCE_DELTA0(NLINES,DELTA_BR,p,pref)
        DELTA += DELTA_BR*fraction
        
    #print('ABSCOEF_FAST: %f sec elapsed for transforming parameters'%(time()-t))
        
    t = time()
    if not test:
        #print('Omegas',type(Omegas),Omegas.dtype,
        #      'NU',type(NU),NU.dtype,
        #      'SW',type(SW),SW.dtype,
        #      'ELOWER',type(ELOWER),ELOWER.dtype,
        #      'MOLEC_ID',type(MOLEC_ID),MOLEC_ID.dtype,
        #      'LOCAL_ISO_ID',type(LOCAL_ISO_ID),LOCAL_ISO_ID.dtype,
        #      'GAMMA_L',type(GAMMA_L),GAMMA_L.dtype,
        #      'GAMMA_D',type(GAMMA_D),GAMMA_D.dtype,
        #      'DELTA',type(DELTA),DELTA.dtype,
        #      'NLINES',type(NLINES),None,
        #      'OmegaRange',type(OmegaRange),OmegaRange.dtype,
        #      'OmegaStep',type(OmegaStep),None,
        #      'OmegaWing',type(OmegaWing),None,
        #      'OmegaWingHW',type(OmegaWingHW),None,
        #      'reflect',type(reflect),None,
        #      'profile',type(profile),None)
        Xsect = CALC_(Omegas,NU,SW,ELOWER,MOLEC_ID,LOCAL_ISO_ID,GAMMA_L,GAMMA_D,DELTA,NLINES,
                    OmegaWing,OmegaWingHW,reflect,profile)    
        #Xsect = CALC0_(Omegas,NU,SW,ELOWER,MOLEC_ID,LOCAL_ISO_ID,GAMMA_L,GAMMA_D,DELTA,NLINES,
        #            OmegaWing,OmegaWingHW,reflect,profile,NCORES)    
    else:
        Xsect = CALC_test(Omegas,NU,SW,ELOWER,MOLEC_ID,LOCAL_ISO_ID,GAMMA_L,GAMMA_D,DELTA,NLINES,
                    OmegaWing,OmegaWingHW,reflect,profile)    
    #print('ABSCOEF_FAST: %f sec elapsed for executing CALC_'%(time()-t))

    return Omegas,Xsect
    
# PROBLEMS WITH THE "EXPERIMENTAL" PARALLEL=TRUE FEATURE
# https://github.com/numba/numba/issues/2804
# https://stackoverflow.com/questions/35459065/numbas-parallel-vectorized-functions
# https://stackoverflow.com/questions/46009368/usage-of-parallel-option-in-numba-jit-decoratior-makes-function-give-wrong-resul
    
#@autojit
#@njit
#@njit(parallel=PARALLEL,fastmath=FASTMATH)
## ! When specifying the types explicitly, the compilation becomes much faster
##                            Omegas         NU                SW               ELOWER          MOLEC_ID    LOCAL_ISO_ID
# @njit([numba.float64[:](numba.float64[:],numba.float64[:],numba.float64[:],numba.float64[:],numba.int32[:],numba.int32[:],
# #        GAMMA_L          GAMMA_D          DELTA             NLINES       OmegaRange
       # numba.float64[:],numba.float64[:],numba.float64[:],numba.int64,numba.float64[:],
# #       OmegaStep      OmegaWing    OmegaWingHW    reflect       profile=1
       # numba.float64,numba.float64,numba.float64,numba.boolean,numba.int64)],
       # parallel=PARALLEL,fastmath=FASTMATH)
@njit(parallel=PARALLEL,fastmath=FASTMATH)
def CALC_(Omegas,NU,SW,ELOWER,MOLEC_ID,LOCAL_ISO_ID,GAMMA_L,GAMMA_D,DELTA,NLINES,
          OmegaWing=None,OmegaWingHW=None,reflect=True,profile=1):
    # THIS FUNCTION SUFFERS FROM THE PARALLELIZATION BUG IN NUMBA 
    # WHEN THE CONCURRENT ADDITION TO THE ARRAY GIVES RUBBISH RESULT.
    # THIS BUT WAS FIXED (AS STATED) ONLY IN NUMBA V.0.40
    # https://github.com/numba/numba/issues/2804
          
    # PROFILES: 1 - Voigt, 2 - Lorentz, 3 - Doppler
   
    OmegaRange = (Omegas[0],Omegas[-1])
   
    number_of_points = len(Omegas)
    Xsect = np.zeros(number_of_points,dtype=np.float64)
    
    #print('<<<<<<<<<<< CALC_>>>>>>>>>>>>>')
    #print('number_of_points: '); print(number_of_points)
                         
    # loop through line centers (single stream)
    #for RowID in range(NLINES):
    for RowID in prange(NLINES): # vectorizing the loop    
        
        # get basic line parameters (lower level)
        LineCenterDB = NU[RowID]
        LineIntensityDB = SW[RowID]
        LowerStateEnergyDB = ELOWER[RowID]
        MoleculeNumberDB = MOLEC_ID[RowID]
        IsoNumberDB = LOCAL_ISO_ID[RowID]
        Gamma0 = GAMMA_L[RowID]
        GammaD = GAMMA_D[RowID]
        Shift0 = DELTA[RowID]
        
        #   get final wing of the line according to Gamma0, OmegaWingHW and OmegaWing
        #OmegaWingF = max(OmegaWing,OmegaWingHW*Gamma0,OmegaWingHW*GammaD)
        OmegaWingF = np.max(np.array([OmegaWing,OmegaWingHW*Gamma0,OmegaWingHW*GammaD]))
        
        # check if the line calculation range overlaps with the given global calculation range
        if LineCenterDB+Shift0+OmegaWingF<OmegaRange[0] or LineCenterDB+Shift0-OmegaWingF>OmegaRange[1]:
            #print('1>>>>>')
            continue
        
        LineIntensity = LineIntensityDB
        
        BoundIndexLower = np.searchsorted(Omegas,LineCenterDB-OmegaWingF,side='right') # side='right' makes new code consistent with old HAPI
        BoundIndexUpper = np.searchsorted(Omegas,LineCenterDB+OmegaWingF,side='right') # side='right' makes new code consistent with old HAPI
        if reflect: # calculate only half of the profile
            #BoundIndexMiddl = (BoundIndexUpper-BoundIndexLower)//2 # ATTENTION: n/2 gives float result in Python 3!!! use the integer division operator
            BoundIndexMiddl = np.searchsorted(Omegas,LineCenterDB-Gamma0*1,side='right')-BoundIndexLower
        else: # calculate on the full grid
            BoundIndexMiddl = 0
        
        omega_vals = Omegas[BoundIndexLower:BoundIndexUpper]
        lineshape_vals = np.zeros(BoundIndexUpper-BoundIndexLower)
        
        #print('2>>>>>')
        
        #print(
        #    'RowID>>',RowID,'LineCenterDB: ',LineCenterDB,'\n',
        #    'RowID>>',RowID,'LineIntensityDB: ',LineIntensityDB,'\n',
        #    'RowID>>',RowID,'LowerStateEnergyDB: ',LowerStateEnergyDB,'\n',
        #    'RowID>>',RowID,'MoleculeNumberDB: ',MoleculeNumberDB,'\n',
        #    'RowID>>',RowID,'IsoNumberDB: ',IsoNumberDB,'\n',
        #    'RowID>>',RowID,'Gamma0: ',Gamma0,'\n',
        #    'RowID>>',RowID,'GammaD: ',GammaD,'\n',
        #    'RowID>>',RowID,'Shift0: ',Shift0,'\n',
        #    'RowID>>',RowID,'OmegaWingF: ',OmegaWingF,'\n',
        #    'RowID>>',RowID,'BoundIndexLower: ',BoundIndexLower,'\n',
        #    'RowID>>',RowID,'BoundIndexUpper: ',BoundIndexUpper,'\n',
        #    'RowID>>',RowID,'min(Omegas): ',np.min(Omegas),'max(Omegas): ',np.max(Omegas),'\n',
        #    'RowID>>',RowID,'LineCenterDB-OmegaWingF: ',LineCenterDB-OmegaWingF,'\n',
        #    'RowID>>',RowID,'LineCenterDB+OmegaWingF: ',LineCenterDB+OmegaWingF,'\n',
        #    'RowID>>',RowID,'len(omega_vals): ',len(omega_vals),'\n',
        #    'RowID>>',RowID,'len(lineshape_vals): ',len(lineshape_vals),'\n',
        #)
        
        #lineshape_vals[BoundIndexMiddl:] = PROFILE(LineCenterDB,GammaD,Gamma0,0.0,Shift0,0.0,omega_vals[BoundIndexMiddl:])[0] # numba version
        #print('starting profile calculation')
        if profile==1:
            lineshape_vals[BoundIndexMiddl:] = PROFILE_SDVOIGT(LineCenterDB,GammaD,Gamma0,0.0,Shift0,0.0,omega_vals[BoundIndexMiddl:])[0] # numba version
        elif profile==2:
            lineshape_vals[BoundIndexMiddl:] = PROFILE_LORENTZ(LineCenterDB+Shift0,Gamma0,omega_vals[BoundIndexMiddl:]) # numba version
        elif profile==3:
            lineshape_vals[BoundIndexMiddl:] = PROFILE_DOPPLER(LineCenterDB,GammaD,omega_vals[BoundIndexMiddl:]) # numba version
        else:
            print('  ~~ UNKNOWN PROFILE NUMBER')
        #print('done profile calculation')
            
        if reflect:
            #n = len(lineshape_vals); i = n//2; lineshape_vals[:i] = lineshape_vals[:i-1+n%2:-1] # THIS NEEDS DEBUGGING; ATTENTION: n/2 gives float result in Python 3!!! use the integer division operator
            n = len(lineshape_vals); lineshape_vals[:BoundIndexMiddl]=lineshape_vals[n-1:n-BoundIndexMiddl-1:-1] 
        # reflect line shape values if they have been calculated on a half-grid
        
        Xsect[BoundIndexLower:BoundIndexUpper] += LineIntensity * lineshape_vals
    
    #return Omegas,Xsect
    return Xsect

@njit(parallel=PARALLEL,fastmath=FASTMATH)
def CALC0_(Omegas,NU,SW,ELOWER,MOLEC_ID,LOCAL_ISO_ID,GAMMA_L,GAMMA_D,DELTA,NLINES,
          OmegaWing=None,OmegaWingHW=None,reflect=True,profile=1,NCORES=1):
    # THIS VERSION OF CALC USES DIFFERENT PARALLELIZATION SCHEME...
    # ...MADE TO WORKAROUND THE PARALLELIZATION BUG IN NUMBA<0.40 VERSIONS
          
    # PROFILES: 1 - Voigt, 2 - Lorentz, 3 - Doppler
   
    #Omegas = np.arange(OmegaRange[0],OmegaRange[1],OmegaStep)
    #Omegas = arange_(OmegaRange[0],OmegaRange[1],OmegaStep)
    number_of_points = len(Omegas)
    Xsect = np.zeros(number_of_points,dtype=np.float64)
    
    #print('<<<<<<<<<<< CALC_>>>>>>>>>>>>>')
    #print('number_of_points: '); print(number_of_points)

    delta = number_of_points//NCORES
                        
    for coreID in prange(NCORES): # parallel loop, coarser grain
        # get indexes for lower and upper wavenumber grid bounds
        i = delta*coreID
        j = delta*(coreID+1)
        if coreID==NCORES-1 and j<number_of_points:
            j = number_of_points

        #for RowID in range(NLINES): # sequential loop, fine grain
        RowID = 0
        while RowID<NLINES:
            
            # get basic line parameters (lower level)
            LineCenterDB = NU[RowID]
            LineIntensityDB = SW[RowID]
            LowerStateEnergyDB = ELOWER[RowID]
            MoleculeNumberDB = MOLEC_ID[RowID]
            IsoNumberDB = LOCAL_ISO_ID[RowID]
            Gamma0 = GAMMA_L[RowID]
            GammaD = GAMMA_D[RowID]
            Shift0 = DELTA[RowID]
            
            RowID += 1
        
            #print('LineCenterDB: '); print(LineCenterDB)
            #print('LineIntensityDB: '); print(LineIntensityDB)
            #print('LowerStateEnergyDB: '); print(LowerStateEnergyDB)
            #print('MoleculeNumberDB: '); print(MoleculeNumberDB)
            #print('IsoNumberDB: '); print(IsoNumberDB)
            #print('Gamma0: '); print(Gamma0)
            #print('GammaD: '); print(GammaD)
            #print('Shift0: '); print(Shift0)
                                    
            #   get final wing of the line according to Gamma0, OmegaWingHW and OmegaWing
            OmegaWingF = max(OmegaWing,OmegaWingHW*Gamma0,OmegaWingHW*GammaD)
            #OmegaWingF = np.max(np.array([OmegaWing,OmegaWingHW*Gamma0,OmegaWingHW*GammaD]))
                    
            #print('OmegaWingF: '); print(OmegaWingF)
            
            LineIntensity = LineIntensityDB
        
            BoundIndexLower = np.searchsorted(Omegas,LineCenterDB-OmegaWingF,side='right') # side='right' makes new code consistent with old HAPI
            BoundIndexUpper = np.searchsorted(Omegas,LineCenterDB+OmegaWingF,side='right') # side='right' makes new code consistent with old HAPI
            
            # NEW: get indexes accounting for OmegaWing
            i_ = max(i,BoundIndexLower)
            j_ = min(j,BoundIndexUpper)
            #print(i,j,BoundIndexLower,BoundIndexUpper,i_,j_,Omegas[0],Omegas[-1],LineCenterDB-OmegaWingF,LineCenterDB+OmegaWingF)
            #print(coreID,RowID)
            
            #if i_>j_: 
            #    print('hop!')
            #    continue # if the interval sliced with OmegaWingF doesn't intersect with [i,j]

            # LATER REFLECTION SHOULD BE ADDED
            
            #print('BoundIndexLower: '); print(BoundIndexLower)
            #print('BoundIndexUpper: '); print(BoundIndexUpper)
            
            #omega_vals = Omegas[i_:j_]
            lineshape_vals = np.zeros(BoundIndexUpper-BoundIndexLower)
        
            if profile==1:
                lineshape_vals = PROFILE_SDVOIGT(LineCenterDB,GammaD,Gamma0,0.0,Shift0,0.0,Omegas[i_:j_])[0] # numba version
            elif profile==2:
                lineshape_vals = PROFILE_LORENTZ(LineCenterDB,Gamma0,Omegas[i_:j_]) # numba version
            elif profile==3:
                lineshape_vals = PROFILE_DOPPLER(LineCenterDB,GammaD,Omegas[i_:j_]) # numba version
            else:
                print('  ~~ UNKNOWN PROFILE NUMBER')
            
            #if reflect: # MUST REDO THIS FOR NEW PARALLELIZATION SCHEME!!!
                #n = len(lineshape_vals); i = n//2; lineshape_vals[:i] = lineshape_vals[:i-1+n%2:-1] # THIS NEEDS DEBUGGING; ATTENTION: n/2 gives float result in Python 3!!! use the integer division operator
            # reflect line shape values if they have been calculated on a half-grid
        
            Xsect[i_:j_] += LineIntensity * lineshape_vals 
            #print(np.sum(lineshape_vals**2))
            #print(LineIntensity)
            #print(np.min(lineshape_vals))
            
    #print('>>>>>>')
    #print(np.sum(Xsect**2))
                
    return Xsect
    
# ===========================================
# TEST FOR CORRECTNESS OF THE PARALLELIZATION
# ===========================================

#@autojit
#@njit
#@njit(parallel=PARALLEL,fastmath=FASTMATH)
## ! When specifying the types explicitly, the compilation becomes much faster
##                            Omegas         NU                SW               ELOWER          MOLEC_ID    LOCAL_ISO_ID
#@njit([numba.float64[:](numba.float64[:],numba.float64[:],numba.float64[:],numba.float64[:],numba.int32[:],numba.int32[:],
##        GAMMA_L          GAMMA_D          DELTA             NLINES       OmegaRange
#       numba.float64[:],numba.float64[:],numba.float64[:],numba.int64,numba.float64[:],
##       OmegaStep      OmegaWing    OmegaWingHW    reflect       profile=1
#       numba.float64,numba.float64,numba.float64,numba.boolean,numba.int64)],
#       parallel=PARALLEL,fastmath=FASTMATH)
@njit(parallel=PARALLEL,fastmath=FASTMATH)
def CALC_test(Omegas,NU,SW,ELOWER,MOLEC_ID,LOCAL_ISO_ID,GAMMA_L,GAMMA_D,DELTA,NLINES,
              OmegaRange=None,OmegaStep=None,OmegaWing=None,OmegaWingHW=None,reflect=True,profile=1):
          
    # SIMPLE TEST TO CHECK HOW THE NUMBA'S AUTO-PARALLELIZATION WORKS (SPOILER: VERY BUGGY)
   
    number_of_points = len(Omegas)
    Xsect = np.zeros(number_of_points,dtype=np.float64)
    
    # loop through line centers (single stream)
    #for RowID in range(NLINES):
    for RowID in prange(NLINES): # vectorizing the loop                               
        a = np.ones(number_of_points)
        for i in range(number_of_points):
            Xsect[i] += a[i]
    
    #return Omegas,Xsect
    return Xsect
    
    