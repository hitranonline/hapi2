from hapi2.config import VARSPACE

from .libra2 import *

models = VARSPACE['db_backend'].models
session = VARSPACE['session']

class Container_HAPI2_stored(Container):
    
    def pack(self,obj):
        return self.__contained_class__.pack(obj)
    
    def unpack(self):
        return self.__contained_class__.unpack(self.__buffer__)

class Container_PartitionFunction(Container):
    __contained_class__ = models.PartitionFunction
Container.register(Container_PartitionFunction)

class Container_Molecule(Container_HAPI2_stored):
    __contained_class__ = models.Molecule
Container.register(Container_Molecule)

class Container_Isotopologue(Container_HAPI2_stored):
    __contained_class__ = models.Isotopologue
Container.register(Container_Isotopologue)

class Container_Transition(Container_HAPI2_stored):
    __contained_class__ = models.Transition
Container.register(Container_Transition)

class Container_Linelist(Container_HAPI2_stored):
    __contained_class__ = models.Linelist
Container.register(Container_Linelist)

class Container_Source(Container_HAPI2_stored):
    __contained_class__ = models.Source
Container.register(Container_Source)

class Container_CrossSection(Container_HAPI2_stored):

    __contained_class__ = models.CrossSection

    def unpack(self):
        dct = load_from_string(self.__buffer__)
        return session.query(self.__contained_class__).\
            filter(getattr(self.__contained_class__,'hash').\
                like(self.__hashval__)).first()
        
Container.register(Container_CrossSection)

class Container_CIACrossSection(Container_HAPI2_stored):

    __contained_class__ = models.CIACrossSection

    def unpack(self):
        dct = load_from_string(self.__buffer__)
        return session.query(self.__contained_class__).\
            filter(getattr(self.__contained_class__,'hash').\
                like(self.__hashval__)).first()
                
Container.register(Container_CIACrossSection)

class Container_Mixture(Container_HAPI2_stored):    
    __contained_class__ = models.Mixture
Container.register(Container_Mixture)