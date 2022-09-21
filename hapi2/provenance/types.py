from hapi2.config import VARSPACE

from .libra2 import *

class Container_HAPI2_stored(Container):
    
    def pack(self,obj):
        dct = obj.dump()
        buffer = dump_to_string(dct)
        hashval = calc_hash_dict(dct)
        return buffer, hashval
    
    def unpack(self):
        dct = load_from_string(self.__buffer__)
        return self.__contained_class__.load(dct)

class Container_PartitionFunction(Container):

    __contained_class__ = VARSPACE['db_backend'].models.PartitionFunction

    def pack(self,obj):
        dct = obj.dump()
        dct['__QQ__'] = binascii.hexlify(dct['__QQ__']).decode(ENCODING)
        dct['__TT__'] = binascii.hexlify(dct['__TT__']).decode(ENCODING)
        buffer = dump_to_string(dct)
        hashval = calc_hash_dict(dct)
        return buffer, hashval
    
    def unpack(self):
        dct = load_from_string(self.__buffer__)
        dct['__QQ__'] = binascii.unhexlify(dct['__QQ__'])
        dct['__TT__'] = binascii.unhexlify(dct['__TT__'])  
        return self.__contained_class__.load(dct)

Container.register(Container_PartitionFunction)

class Container_Linelist(Container_HAPI2_stored):
    __contained_class__ = VARSPACE['db_backend'].models.Linelist
Container.register(Container_Linelist)

class Container_Source(Container_HAPI2_stored):
    __contained_class__ = VARSPACE['db_backend'].models.Source
Container.register(Container_Source)

def pack_cross_section(xs):
    dct = xs.dump()
    del dct['id']
    nu,xsc = xs.get_data()
    dct['hash_nu'] = pack_ndarray(nu)[1] if nu is not None else '' 
    dct['hash_xsc'] = pack_ndarray(xsc)[1] if xsc is not None  else '' 
    buffer = dump_to_string(dct)
    hashval = calc_hash_dict(dct)
    return buffer, hashval

class Container_CrossSection(Container):

    __contained_class__ = VARSPACE['db_backend'].models.CrossSection

    def pack(self,obj):
        return pack_cross_section(obj)
    
    def unpack(self):
        dct = load_from_string(self.__buffer__)
        return query(self.__contained_class__).\
            filter(getattr(self.__contained_class__,'hash').\
                like(self.__hashval__)).first()
        
Container.register(Container_CrossSection)

class Container_Mixture(Container):
    
    __contained_class__ = VARSPACE['db_backend'].models.Mixture

    def pack(self,obj):
        dct = Mixture.__dump__(obj)
        buffer = dump_to_string(dct)
        hashval = calc_hash_dict(dct)
        return buffer, hashval
    
    def unpack(self):
        dct = load_from_string(self.__buffer__)
        mix = Mixture.__load__(dct)
        return mix

Container.register(Container_Mixture)