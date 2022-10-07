from . import lbl
from . import xsc
from . import cia

from hapi2.config import VARSPACE        

provenance = VARSPACE['prov_backend']
Container = provenance.Container
Container_HAPI2_stored = provenance.Container_HAPI2_stored

def merge_abundance_dicts(dct1,dct2,a1,a2):
    """ Merge abundance dictionaries with weights. """
    dct_ = {}
    for key in dct1:
        dct_[key] = a1*dct1[key]
    for key in dct2:
        if key in dct_:
            dct_[key] += a2*dct2[key]
        else:
            dct_[key] = a2*dct2[key]
    return dct_

class Serializable: # mocks up the behavior of the persistent classes
    
    @classmethod
    def pack(cls,obj):
        prov = VARSPACE['prov_backend']
        dct = cls.dump(obj) 
        buffer = prov.dump_to_string(dct)
        hashval = prov.calc_hash_dict(dct)
        return buffer, hashval
    
    @classmethod
    def unpack(cls,buffer):
        prov = VARSPACE['prov_backend']
        dct = prov.load_from_string(buffer)
        mix = cls.load(dct)
        return mix

class Conditions(Serializable):
    """ Transient class representing the thermodynamical conditions. """
    
    def __init__(self,T=296,p=1):
        self.dict = dict(T=T,p=p)
    
    @property
    def T(self):
        return self.dict['T']

    @property
    def p(self):
        return self.dict['p']

    @classmethod
    def dump(self,cond):
        return cond.dict

    @classmethod
    def load(self,dct):
        cond = Conditions(dct['T'],dct['p'])
        return cond
        
    def __repr__(self):
        return str(self.dict)

class Mixture(Serializable):
    """ Transient class representing the gas mixture. """
    
    def __init__(self,components,isocomp={}):
        models = VARSPACE['db_backend'].models
        if type(components) is str:
            components = {components:1}
        self.components = components
        self.isocomp = {}
        for comp in components:
            if comp in isocomp:
                self.isocomp[comp] = isocomp[comp]
            else:
                isos = models.Molecule(comp).isotopologues
                if isos:
                    self.isocomp[comp] = {iso.iso_name:iso.abundance for iso in isos}
    
    def __clone__(self):
        components = self.components.copy()
        isocomp = self.isocomp.copy()
        mix = Mixture(components,isocomp)
        return mix
    
    def __mul__(self,vmr):
        mix = self.__clone__()
        for comp in mix.components:
            mix.components[comp] *= vmr
        return mix
    
    def __rmul__(self,vmr):
        return self.__mul__(vmr)

    def __add__(self,mix):
        # merge mix into mix_
        mix_ = self.__clone__()
        mix = mix.__clone__()
        # merge isocomps first
        for molname in mix_.isocomp:
            dct = mix_.isocomp[molname]
            if molname in mix.isocomp:
                mix.isocomp[molname] = merge_abundance_dicts(mix.isocomp[molname],dct,1,1) 
            else:
                mix.isocomp[molname] = dct.copy()
        # merge components
        mix.components = merge_abundance_dicts(mix_.components,mix.components,1,1)
        return mix
        
    def get_component_name(self,mol):
        models = VARSPACE['db_backend'].models
        if '__components_lookup__' not in self.__dict__:
            self.__components_lookup__ = {}
            for compname in self.components:
                self.__components_lookup__[models.Molecule(compname)] = compname
        return self.__components_lookup__[mol]
    
    @classmethod
    def dump(self,mix):
        return {'components':mix.components,'isocomp':mix.isocomp}

    @classmethod
    def load(self,dct):
        mix = Mixture(dct['components'],dct['isocomp'])
        return mix

    def __repr__(self):
        #return 'Mixture(%s)'%(
        #    ', '.join('%s -> %f'%(comp,self.components[comp]) \
        #        for comp in self.components))
        return str(self.components)
                    
class Container_Mixture(Container_HAPI2_stored):    
    __contained_class__ = Mixture
Container.register(Container_Mixture)

class Container_Conditions(Container_HAPI2_stored):    
    __contained_class__ = Conditions
Container.register(Container_Conditions)