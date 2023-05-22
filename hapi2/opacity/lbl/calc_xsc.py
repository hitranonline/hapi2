import json
import hapi

import numpy as np

from functools import reduce
from sqlalchemy import func

from hapi2.config import VARSPACE
from hapi2.db.sqlalchemy.legacy import storage2cache

from . import numba

provenance = VARSPACE['prov_backend']
models = VARSPACE['db_backend'].models

def create_diluent(molname,mixture):
    """ Create the Diluent parameter for the legacy absorption coefficient code.
        Current molecule (molname) will be changed to 'self' to comply with HITRAN format. """
    Diluent = {}
    for compname in mixture.components:
        abun = mixture.components[compname]
        if compname==molname:
            Diluent['self'] = abun
        else:
            compname = compname.lower() # !!!
            Diluent[compname] = abun
    return Diluent

def create_components(isos,mixture):
    """ Create the Components parameter for the legacy absorption coefficient code."""
    mixture_iso_hash = {}
    for molname in mixture.isocomp:
        for isoname in mixture.isocomp[molname]:
            iso = models.Isotopologue(isoname)
            mixture_iso_hash[iso] = mixture.isocomp[molname][isoname]
    Components = []
    for iso in isos:
        abundance = mixture_iso_hash[iso]
        comp = (iso.molecule.id,iso.isoid,abundance)
        Components.append(comp)
    return Components

def get_pfunction_lambda(src):
    """ Get the input lambda partition function for the given set of isotopologues """
    
    iso_hash = {}
    for pfunc in src.partition_functions:
        M = pfunc.isotopologue.molecule.id
        I = pfunc.isotopologue.isoid
        iso_hash[(M,I)] = pfunc
    
    pfunc_lambda = lambda M,I,T: iso_hash[(M,I)].Q(T)
    
    return pfunc_lambda

def get_wavenumber_grid(options,linelist):
    """ Get the grid from the input parameters of the legacy absorption coefficient code."""
    WavenumberGrid = options.get('WavenumberGrid')
    if WavenumberGrid is None:
        WavenumberRange = options.get('WavenumberRange')
        WavenumberStep = options.get('WavenumberStep',0.01)
        if WavenumberRange is None:
            WavenumberRange = linelist.transitions.with_entities(
                func.min(models.Transition.nu),func.max(models.Transition.nu)).first()
        WavenumberGrid = hapi.arange_(*WavenumberRange,WavenumberStep)
    return WavenumberGrid

@provenance.track(nout=1,cache=False,autosave=False)
def LBL_CALC(
        linelist,
        mixture,
        conditions,
        pfunction_source,
        profile,
        calcpars,
        lbl_backend,
        options,
    ):
    
    # Calculate abscoefs for each molecule separately to avoid the "self" bug.
    
    Environment = conditions.dict
      
    wngrid = get_wavenumber_grid(options,linelist)
    xsc = np.zeros(len(wngrid))
    
    #print('\nwngrid>>>',wngrid)
    
    mols = sorted(linelist.molecules,key=lambda mol:mol.id)

    pfunc = get_pfunction_lambda(pfunction_source)

    options_ = options.copy()
    options_.update(dict(
        partitionFunction=pfunc,
        Environment=Environment,
        WavenumberGrid=np.array(wngrid),
    ))

    #print('\noptions_',options_)

    linelist_isos = linelist.isotopologues


    for mol in mols:
                
        molname = mixture.get_component_name(mol)
        isos = set(linelist_isos).intersection(mol.isotopologues)
               
        Components = create_components(isos,mixture)
        SourceTables = '~scratch'
                
        Diluent = create_diluent(molname,mixture)
        
        isoals = reduce(lambda x,y:x+y,[iso.aliases for iso in isos])
        isoal_ids = [al.id for al in isoals]
        query = linelist.transitions.filter(
            models.Transition.isotopologue_alias_id.in_(isoal_ids))
        storage2cache(SourceTables,query=query)
        
        _,xsc_ = lbl_backend.absorptionCoefficient_Generic(**options_,
            Components=Components,SourceTables=SourceTables,Diluent=Diluent,
            profile=profile,calcpars=calcpars)
        
        xsc += xsc_

    # The pure molecule can not be assigned if the linelist contains 
    # lines of multiple molecules mixed together. 
    # In this case, use the molecule "stub" with the specific alias,
    # for example:  "|Water|Carbon Dioxide|Methane|"
    if len(mols)==1:
        mol = models.Molecule(molname)
    else:
        mol = models.Molecule('|%s|'%('|'.join([mol.common_name for mol in mols])))
    
    # Create dummy source
    src = models.Source('LBL')
    
    xs = models.CrossSection(
        molecule=mol,source=src,
        temperature=conditions.T,
        pressure=conditions.p,
        npnts=len(wngrid),
        sigma_max=max(xsc),
    )
    xs.set_data(wngrid,xsc)
    
    return xs
