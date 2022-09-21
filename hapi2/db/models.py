import re
import numpy as np

from hapi2.config import VARSPACE        

from hapi2.utils.formula import molweight,atoms,natoms

from hapi2.utils.xsc import compress_zlib, decompress_zlib, \
    pack_double, unpack_double, pack_float, unpack_float

from hapi import putRowObjectToString,HITRAN_DEFAULT_HEADER, AtoB

def get_alias_class(cls):
    """
    Get the suitable alias class for a proper aliased class.
    """
    return getattr(VARSPACE['db_backend'].models,
        cls.__backrefs__['aliases']['class'])

def searchable(Cls):
    """
    Decorator for aliases.
    """    
    @classmethod
    def get(cls,name):
        cls_ = getattr(VARSPACE['db_backend'].models,cls.__name__)
        return cls_.get(name)

    def __init__(self,name):
        if self in VARSPACE['session']: return
        self.alias = name

    def __new__(cls,name=None):
        if name is None:
            return super(Cls, cls).__new__(cls)
        alias = Cls.get(name)
        if alias is None:
            return super(Cls, cls).__new__(cls)
        else:
            return alias  
            
    def __str__(self):
        return str(self.name)

    def __repr__(self):
        return self.__str__()

    Cls.get = get
    Cls.__init__ = __init__
    Cls.__new__ = __new__
    Cls.__str__ = __str__
    Cls.__repr__ = __repr__

    return Cls

def searchable_by_alias(Cls):
    """
    Decorator for "aliased" classes.
    """    
    @classmethod
    def get(cls,name):
        cls_ = getattr(VARSPACE['db_backend'].models,cls.__name__)
        return cls_.get(name)

    def __init__(self,name=None,**kwargs):
        if self in VARSPACE['session']: return
        if type(name) is not str:
            raise Exception('name must be a string')
        al = get_alias_class(self.__class__)(name); al.type = 'generic'
        self.aliases.append(al)
        self.__set_name__(name)
        for key in kwargs:
            setattr(self,key,kwargs[key])

    def __new__(cls,name=None,**kwargs): # don't change!!!
        if name is None:
            return super(Cls, cls).__new__(cls)
        obj = Cls.get(name)
        if obj is None:
            return super(Cls, cls).__new__(cls)
        else:
            return obj
            
    def __str__(self):
        return self.__get_name__()

    def __repr__(self):
        return self.__str__()
            
    Cls.get = get
    Cls.__init__ = __init__
    Cls.__new__ = __new__
    Cls.__str__ = __str__
    Cls.__repr__ = __repr__

    return Cls

class CRUD:
    """
    Implements helper funcs for (C)reate, (R)ead, (U)pdate, and (D)elete.
    """

    @classmethod
    def __check_types__(cls,header):
        """
        Check if supplied class in header coincides with cls.
        """
        supplied_classname = header['content']['class']
        expected_classname = cls.__name__
        if expected_classname != supplied_classname:
            raise Exception('Class mismatch: expected %s, got %s'%\
                (expected_classname,supplied_classname))

    @classmethod
    def update(cls,dct):
        """
        Update existing entries or create new in if not found.
        """
        raise NotImplementedError

    @classmethod
    def read(cls,filter=None,colnames=None):
        """
        Read columns of data from the database.
        """
        raise NotImplementedError
        
    def dump(self):
        """
        Dump serialized keys given in __keys__
        field to the dictionary object.
        """
        dct = {}
        dct['__class__'] = self.__class__.__name__
        dct['__identity__'] = self.__identity__
        exclude = set()
        for refname in self.__refs__:
            dct[refname] = str( getattr(self,refname) )
            for key,_ in self.__refs__[refname]['join']:
                exclude.add(key)
        for key,_ in self.__keys__:
            if key not in exclude:
                dct[key] = getattr(self,key)
        return dct

    @classmethod
    def construct(cls,dct):
        """
        Construct a new object from the input dictionary.
        """
        models = VARSPACE['db_backend'].models
        obj = cls(dct[cls.__identity__])
        for refname in cls.__refs__:
            ref_cls = getattr(models,cls.__refs__[refname]['class'])
            ref_obj = ref_cls(dct[refname])
            setattr(obj,refname,ref_obj)
        return obj

    @classmethod
    def load(self,dct):
        """
        Load object fields given in __keys__ 
        from a dictionary object without any nested stuff.
        """
        raise NotImplementedError
                        
    def __lt__(self,obj):
        raise NotImplementedError
        
    def save(self):
        raise NotImplementedError
            
    @classmethod
    def all(cls):
        raise NotImplementedError

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

class Mixture:
    """ Transient class representing the gas mixture. """
    
    def __init__(self,components,isocomp={}):
        if type(components) is str:
            components = {components:1}
        self.components = components
        self.isocomp = {}
        for comp in components:
            if comp in isocomp:
                self.isocomp[comp] = isocomp[comp]
            else:
                isos = Molecule(comp).isotopologues
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
        if '__components_lookup__' not in self.__dict__:
            self.__components_lookup__ = {}
            for compname in self.components:
                self.__components_lookup__[Molecule(compname)] = compname
        return self.__components_lookup__[mol]
    
    @classmethod
    def __dump__(self,mix):
        return {'components':mix.components,'isocomp':mix.isocomp}

    @classmethod
    def __load__(self,dct):
        mix = Mixture(dct['components'],dct['isocomp'])
        return mix

    def __repr__(self):
        return 'Mixture(%s)'%(
            ', '.join('%s -> %f'%(comp,self.components[comp]) \
                for comp in self.components))
                    
class PartitionFunction:
    """ Follows the definition of Partition Function given in 
        Gamache RR, et al. DOI:10.1016/j.jqsrt.2017.03.045 """

    __keys__ = (
        ('id',                     {'type':int}),
        ('isotopologue_alias_id',  {'type':int}),
        ('source_alias_id',        {'type':int}),
        #('Q296',                   {'type':float}),
        ('tmin',                   {'type':float}),
        ('tmax',                   {'type':float}),
        ('__TT__',                 {'type':str}),
        ('__QQ__',                 {'type':str}),
        ('json',                   {'type':str}),
        ('filename',               {'type':str}),
        ('comment',                {'type':str}),
        ('status',                 {'type':str}),
    )

    __identity__ = 'id'
    
    __refs__ = {
        'isotopologue_alias': {
            'class':'IsotopologueAlias',
            'table':'isotopologue_alias',
            'join':[('isotopologue_alias_id','id'),],
            'backpop':'partition_functions',
        },
        'source_alias': {
            'class':'SourceAlias',
            'table':'source_alias',
            'join':[('source_alias_id','id'),],
            'backpop':'partition_functions',
        },
    }

    __backrefs__ = {}

    def __init__(self,isotopologue,source,**kwargs):
        # set isotopologue alias
        if isotopologue in VARSPACE['session']:
            self.isotopologue_alias = IsotopologueAlias(isotopologue.iso_name)
        else:
            self.isotopologue_alias = isotopologue.aliases[0]
        # set source alias
        if source in VARSPACE['session']:
            self.source_alias = SourceAlias(source.short_alias)
        else:
            self.source_alias = source.aliases[0]
        # set the rest of parameters
        keys = [key for key,_ in self.__keys__]
        keys_valid = set(keys).intersection(kwargs.keys())
        for key in keys_valid:
            setattr(self,key,kwargs[key])

    @classmethod
    def construct(cls,dct):
        models = VARSPACE['db_backend'].models
        dct = dct.copy()
        # get isotopologue alias
        isoname = dct.pop('source_alias')
        iso = models.Isotopologue(isoname)
        # get source
        srcname = dct.pop('source_alias')
        src = models.Source(srcname)
        # construct object
        obj = models.PartitionFunction(isotopologue=iso,source=src,**dct)
        #VARSPACE['session'].expunge(obj)
        return obj

    @property
    def isotopologue(self):
        return self.isotopologue_alias.isotopologue

    @property
    def molecule(self):
        return self.isotopologue.molecule

    @property
    def source(self):
        return self.source_alias.source

    def __call__(self,T):
        return self.Q(T)

    def Q(self,T):
        if 'data' not in self.__dict__:
            self.data = self.get_data()
        TT = self.data[0]
        QQ = self.data[1]
        Tmin = self.tmin
        Tmax = self.tmax
        if T<Tmin or T>Tmax:
            raise Exception('out of temperature range: %s'%str((Tmin,Tmax)))
        Qt = AtoB(T,TT,QQ,len(TT))
        return Qt
        
    def set_data(self,TT,QQ):
        self.__TT__ = compress_zlib(pack_double(TT))
        self.__QQ__ = compress_zlib(pack_double(QQ))

    def get_data(self):
        TT = None; QQ = None
        if self.__TT__: TT = np.array(unpack_double(decompress_zlib(self.__TT__)))
        if self.__QQ__: QQ = np.array(unpack_double(decompress_zlib(self.__QQ__)))
        return TT,QQ

    def __str__(self):
        return '%s : %s'%(self.source_alias,self.isotopologue_alias)
    
    def __repr__(self):
        return self.__str__()

class CrossSectionData:
    """
    Stores the actual data for the header given in CrossSection.
    Wave numbers are packed in 8-byte double precision,
    while absorption cross-section is packed in 4-byte single precision.
    Single precision for absorption section gives around 5e-6 percent
    difference in accuracy, but reduces the database size twice.
    For wave numbers, single precision generally is not enough.
    """
    
    def __init__(self,nu=None,xsc=None):
        if nu is not None: self.pack_nu(nu)
        if xsc is not None: self.pack_xsc(xsc)
    
    def unpack_nu(self):
        data_nu = self.__nu__
        if not data_nu and \
           self.header.numin and \
           self.header.numax and \
           self.header.npnts:
            return np.linspace(self.header.numin,
                self.header.numax,self.header.npnts)
        else:
            try:
                return np.array(unpack_double(decompress_zlib(data_nu)))
            except Exception as e:
                print('nu is empty: %s'%e)
                return None

    def unpack_xsc(self):
        data_xsc = self.__xsc__
        if not data_xsc:
            print('xsc is empty')
            return None
        else:
            return np.array(unpack_double(decompress_zlib(data_xsc)))

    def pack_nu(self,nu):
        self.__nu__ = compress_zlib(pack_double(nu))

    def pack_xsc(self,xsc):
        self.__xsc__ = compress_zlib(pack_double(xsc))

class CrossSection:

    __keys__ = (
        ('id',                 {'type':int}),
        ('molecule_alias_id',  {'type':int}),
        ('source_alias_id',    {'type':int}),
        ('numin',              {'type':float}),
        ('numax',              {'type':float}),
        ('npnts',              {'type':int}),
        ('sigma_max',          {'type':float}),
        ('temperature',        {'type':float}),
        ('pressure',           {'type':float}),
        ('resolution',         {'type':float}),
        ('resolution_units',   {'type':str}),
        ('broadener',          {'type':str}),
        ('description',        {'type':str}),
        ('apodization',        {'type':str}),
        ('json',               {'type':str}),
        ('filename',           {'type':str}),
        ('format',             {'type':str}),
        ('status',             {'type':str}),
    )

    __identity__ = 'id'
    
    __refs__ = {
        'molecule_alias': {
            'class':'MoleculeAlias',
            'table':'molecule_alias',
            'join':[('molecule_alias_id','id'),],
            'backpop':'cross_sections',
        },
        'source_alias': {
            'class':'SourceAlias',
            'table':'source_alias',
            'join':[('source_alias_id','id'),],
            'backpop':'cross_sections',
        },
    }

    __backrefs__ = {}

    def __init__(self,molecule,source,**kwargs):
        # set molecule alias
        if molecule in VARSPACE['session']:
            self.molecule_alias = MoleculeAlias(molecule.common_name)
        else:
            self.molecule_alias = molecule.aliases[0]
        # set source alias
        if source in VARSPACE['session']:
            self.source_alias = SourceAlias(source.short_alias)
        else:
            self.source_alias = source.aliases[0]
        # set the rest of parameters
        keys = [key for key,_ in self.__keys__]
        keys_valid = set(keys).intersection(kwargs.keys())
        for key in keys_valid:
            setattr(self,key,kwargs[key])

    @classmethod
    def construct(cls,dct):
        models = VARSPACE['db_backend'].models
        dct = dct.copy()
        # get molecule alias
        molname = dct.pop('molecule_alias')
        mol = models.Molecule(molname)
        # get source
        srcname = dct.pop('source_alias')
        src = models.Source(srcname)
        # construct object
        obj = models.CrossSection(molecule=mol,source=src,**dct)
        #VARSPACE['session'].expunge(obj)
        return obj
        
    @property
    def molecule(self):
        return self.molecule_alias.molecule

    @property
    def source(self):
        return self.source_alias.source

    def set_data(self,nu=None,xsc=None):
        if xsc is None:
            raise Exception('xsc must be non-empty')
        if nu is None:
            if not (self.numin or self.numax):
                raise Exception('numin and numax must be non-empty')
            self.npnts = len(xsc)
        elif len(nu)!=len(xsc):
            raise Exception('nu and xsc must have the same length')
        self.data = VARSPACE['db_backend'].models.CrossSectionData(nu,xsc)
        if nu is not None:
            #self.data.pack_nu(nu)
            self.numin = min(nu)
            self.numax = max(nu)
        #self.data.pack_xsc(xsc)

    def get_data(self):
        """
        Get the spectral data from the "data" relation.
        """
        # Search for the data blob first.
        if self.data:
            return self.data.unpack_nu(), self.data.unpack_xsc()
        return None,None

    def range(self,numin=None,numax=None):
        """
        Get the part of the cross section
        lying within the given wave number range.
        """
        from bisect import bisect
        nu,xsc = self.get_data()
        if numin is None: numin = min(nu) - 10
        if numax is None: numax = max(nu) + 10
        nu = np.array(nu); xsc = np.array(xsc)
        sort_ind = np.argsort(nu) # works faster than the latter option
        nu = nu[sort_ind]; xsc = xsc[sort_ind]
        i1 = bisect(nu,numin); i2 = bisect(nu,numax)
        nu_cut = nu[i1:i2]; xsc_cut = xsc[i1:i2]
        return nu_cut,xsc_cut

    def subset(self,numin,numax):
        """
        The same as range, but creates an new CrossSection object.
        """
        raise NotImplementedError

    def S_int(self,numin,numax):
        """
        Calculate integrated intensity in
        the given spectral region.
        """
        from scipy.integrate import trapz
        nu_cut,xsc_cut = self.range(numin,numax)
        return trapz(xsc_cut,nu_cut)

    def interpolate(self,grid,clean=False):
        from scipy.interpolate import interp1d,Akima1DInterpolator,PchipInterpolator
        INTERPOLATOR = Akima1DInterpolator # better then Pchip if remove close points!
        nu,xsc = self.range(grid[0]-10,grid[-1]+10) # take slightly more wide region for interpolation.
        if clean:
            nu,xsc = average_same_points(nu,xsc)
            nu = np.array(nu); xsc = np.array(xsc)
        interp = INTERPOLATOR(nu,xsc)
        return interp(grid)

    def downsample(self,delta,numin=None,numax=None,type='triangular'):
        nu,xsc = self.range(numin,numax)
        binned_nu,binned_xsc = downsample(nu,xsc,delta,type)
        return binned_nu,binned_xsc

    def compare(self,xs,numin,numax,grid=None):
        raise NotImplementedError

    def __str__(self):
        return '%s : %s'%(self.source_alias,self.molecule_alias)
    
    def __repr__(self):
        return self.__str__()

class CIACrossSection(CrossSection):

    __keys__ = (
        ('id',                         {'type':int}),
        ('collision_complex_alias_id', {'type':int}),
        ('source_alias_id',            {'type':int}),
        ('local_ref_id',               {'type':int}),
        ('numin',                      {'type':float}),
        ('numax',                      {'type':float}),
        ('npnts',                      {'type':int}),
        ('cia_max',                    {'type':float}),
        ('temperature',                {'type':float}),
        ('resolution',                 {'type':float}),
        ('resolution_units',           {'type':str}),
        ('comment',                    {'type':str}),
        ('description',                {'type':str}),
        ('apodization',                {'type':str}),
        ('json',                       {'type':str}),
        ('filename',                   {'type':str}),
        ('format',                     {'type':str}),
        ('status',                     {'type':str}),
    )

    __identity__ = 'id'
    
    __refs__ = {
        'collision_complex_alias': {
            'class':'CollisionComplexAlias',
            'table':'collision_complex_alias',
            'join':[('collision_complex_alias_id','id'),],
            'backpop':'cia_cross_sections',
        },
        'source_alias': {
            'class':'SourceAlias',
            'table':'source_alias',
            'join':[('source_alias_id','id'),],
            'backpop':'cia_cross_sections',
        },
    }

    __backrefs__ = {}

    def __init__(self,collision_complex,source,**kwargs):
        # set collision complex alias
        if molecule in VARSPACE['session']:
            self.collision_complex_alias = CollisionComplexAlias(
                collision_complex.chemical_symbol)
        else:
            self.collision_complex_alias = collision_complex.aliases[0]
        # set source alias
        if source in VARSPACE['session']:
            self.source_alias = SourceAlias(source.short_alias)
        else:
            self.source_alias = source.aliases[0]
        # set the rest of parameters
        keys = [key for key,_ in self.__keys__]
        keys_valid = set(keys).intersection(kwargs.keys())
        for key in keys_valid:
            setattr(self,key,kwargs[key])

    @classmethod
    def construct(cls,dct):
        models = VARSPACE['db_backend'].models
        dct = dct.copy()
        # get molecule alias
        ccompname = dct.pop('collision_complex')
        ccomp = models.CollisionComplex(ccompname)
        # get source
        srcname = dct.pop('source_alias')
        src = models.Source(srcname)
        # construct object
        obj = models.CIACrossSection(molecule=mol,source=src,**dct)
        #VARSPACE['session'].expunge(obj)
        return obj

    @property
    def molecule(self):
        raise Exception('obsolete method for CIACrossSection') 

    @property
    def collision_complex(self):
        return self.collision_complex_alias.collision_complex 

    @property
    def source(self):
        return self.source_alias.source
        
    def set_data(self,nu=None,xsc=None):
        if xsc is None:
            raise Exception('xsc must be non-empty')
        if nu is None:
            if not (self.numin or self.numax):
                raise Exception('numin and numax must be non-empty')
            self.npnts = len(xsc)
        elif len(nu)!=len(xsc):
            raise Exception('nu and xsc must have the same length')
        self.data = VARSPACE['db_backend'].models.CIACrossSectionData(nu,xsc)
        if nu is not None:
            #self.data.pack_nu(nu)
            self.numin = min(nu)
            self.numax = max(nu)
        #self.data.pack_xsc(xsc)

    def __str__(self):
        return '%s : %s'%(self.source_alias,self.collision_complex_alias)
    
    def __repr__(self):
        return self.__str__()

@searchable
class SourceAlias:

    __keys__ = (
        ('id',         {'type':int}),
        ('source_id',  {'type':int}),
        ('alias',      {'type':str}),
        ('type',       {'type':str}),
    )
        
    __identity__ = 'alias'

    #__refs__ = {}
    __refs__ = {
        'source': {
            'class':'Source',
            'table':'source',
            'join':[('source_id','id'),],
            'backpop':'aliases',
        },
    }

    __backrefs__ = {}

    @property
    def name(self):
        return self.alias

@searchable_by_alias
class Source:

    __keys__ = (
        ('id',           {'type':int}),
        ('type',         {'type':str}),
        ('authors',      {'type':str}),
        ('title',        {'type':str}),
        ('journal',      {'type':str}),
        ('volume',       {'type':str}),
        ('page_start',   {'type':str}),
        ('page_end',     {'type':str}),
        ('year',         {'type':int}),
        ('institution',  {'type':str}),
        ('note',         {'type':str}),
        ('doi',          {'type':str}),
        ('bibcode',      {'type':str}),
        ('url',          {'type':str}),
        ('short_alias',  {'type':str}),
    )

    #__identity__ = 'id'
    __identity__ = 'short_alias'
    
    __refs__ = {}

    __backrefs__ = {
        'aliases': {
            'class':'SourceAlias',
            'table':'source_alias',
            'join':[('id','source_id'),],
            'backpop':'source',
        },
    }

    #@property
    #def name(self):
    #    return self.short_alias
    
    def __set_name__(self,name):
        self.short_alias = name
        
    def __get_name__(self):
        return self.short_alias

    @property
    def first_alias(self):
        raise NotImplementedError

    @staticmethod
    def first(srcname=None):
        raise NotImplementedError

    @property
    def cross_sections(self):
        xss = set()
        for source_alias in self.aliases:
            xss.update(source_alias.cross_sections)
        return list(xss)

    @property
    def cia_cross_sections(self):
        xss = set()
        for source_alias in self.aliases:
            xss.update(source_alias.cia_cross_sections)
        return list(xss)
        
    @property
    def partition_functions(self):
        pfuncs = set()
        for source_alias in self.aliases:
            pfuncs.update(source_alias.partition_functions)
        return list(pfuncs)

    @property
    def transition_parameters(self):
        raise NotImplementedError

    @property
    def transitions(self):
        raise NotImplementedError

    def display(self):
        authors = self.authors[:35] if self.authors else None
        title = self.title[:35] if self.title else None
        return '%s // %s // %s // %s'%\
        (authors,title,self.journal,self.year)

    @property
    def citation(self):
        buf = ''
        if self.authors: buf += '%s. '%self.authors
        if self.title: buf += '%s. '%self.title
        if self.journal: buf += '%s '%self.journal
        if self.year: buf += '%d;'%self.year
        if self.volume: buf += '%s:'%self.volume
        if self.page_start: buf += '%s'%self.page_start
        if self.page_end: buf += '-%s. '%self.page_end
        if self.doi: buf += 'doi:%s. '%self.doi
        return buf

    @property
    def short_citation(self): # DELETE
        result = '%s %s'%(self.type.capitalize(),self.id)
        if self.authors:
            authors_split = self.authors.split(',')
            result += ': %s'%authors_split[0]
            if len(authors_split)>1:
                result += ' et al.'
        if self.year:
            result += ' (%s)'%self.year
        return result

@searchable
class ParameterMeta:

    __keys__ = (
        ('id',           {'type':int}),
        ('name',         {'type':str}),
        ('type',         {'type':str}),
        ('description',  {'type':str}),
        ('format',       {'type':str}),
        ('units',        {'type':str}),
    )

    __identity__ = 'name'
    
    __refs__ = {}

    __backrefs__ = {}

@searchable
class Linelist:
  
    __keys__ = (
        ('id',           {'type':int}),
        ('name',         {'type':str}),
        ('description',  {'type':str}),
    )

    __identity__ = 'name'
    
    __refs__ = {}

    __backrefs__ = {}

    @property
    def molecules(self):
        raise NotImplementedError

    @property
    def cross_sections(self):
        raise NotImplementedError

def createRowObject(trans):
    RowObject = []
    for par_name in HITRAN_DEFAULT_HEADER['order']:
        par_value = getattr(trans,par_name)
        par_format = HITRAN_DEFAULT_HEADER['format'][par_name]
        RowObject.append((par_name,par_value,par_format))
    return RowObject

def get_state_key(state):
    return (state.molec_id,state.local_iso_id,*tuple(state.qns.items()))

class Transition:

    __keys__ = (
        ('id',                     {'type':int}),
        ('isotopologue_alias_id',  {'type':int}),
        ('molec_id',               {'type':int}),
        ('local_iso_id',           {'type':int}),
        ('nu',                     {'type':float}),
        ('sw',                     {'type':float}),
        ('a',                      {'type':float}),
        ('gamma_air',              {'type':float}),
        ('gamma_self',             {'type':float}),
        ('elower',                 {'type':float}),
        ('n_air',                  {'type':float}),
        ('delta_air',              {'type':float}),
        ('global_upper_quanta',    {'type':str}),
        ('global_lower_quanta',    {'type':str}),
        ('local_upper_quanta',     {'type':str}),
        ('local_lower_quanta',     {'type':str}),
        ('ierr',                   {'type':str}),
        ('iref',                   {'type':str}),
        ('line_mixing_flag',       {'type':str}),
        ('gp',                     {'type':int}),
        ('gpp',                    {'type':int}),
        ('extra',                  {'type':str}),
    )

    __identity__ = 'id'
    
    __refs__ = {
        'isotopologue_alias': {
            'class':'IsotopologueAlias',
            'table':'isotopologue_alias',
            'join':[('isotopologue_alias_id','id'),],
            'backpop':'transitions',
        },
    }

    __backrefs__ = {}
        
    __states__ = {}

    def __init__(self,isotopologue,**kwargs):
        # set isotopologue alias
        if isotopologue in VARSPACE['session']:
            self.isotopologue_alias = IsotopologueAlias(isotopologue.iso_name)
        else:
            self.isotopologue_alias = isotopologue.aliases[0]
        # set the rest of parameters
        keys = [key for key,_ in self.__keys__]
        keys_valid = set(keys).intersection(kwargs.keys())
        for key in keys_valid:
            setattr(self,key,kwargs[key])

    @classmethod
    def construct(cls,dct):
        models = VARSPACE['db_backend'].models
        dct = dct.copy()
        # get isotopologue alias
        isoname = dct.pop('source_alias')
        iso = models.Isotopologue(isoname)
        # construct object
        obj = models.Transition(isotopologue=iso,**dct)
        #VARSPACE['session'].expunge(obj)
        return obj
        
    @property
    def molecule(self):
        return self.isotopologue_alias.molecule

    @property
    def isotopologue(self):
        return self.isotopologue_alias.isotopologue

    @property
    def source(self):
        return self.source_alias.source

    def __fill_states__(self):
        t = HITRANTransition.parse_par_line(self.par_line)
        statep = t.statep
        statepp = t.statepp
        # search for states in the buffer
        key = get_state_key(statep)
        if key not in Transition.__states__:
            Transition.__states__[key] = statep
        key = get_state_key(statepp)
        if key not in Transition.__states__:
            Transition.__states__[key] = statepp
        # save local links to states
        self.__statep__ = statep
        self.__statepp__ = statepp

    @property
    def statep(self):
        if '__statep__' not in self.__dict__: self.__fill_states__()
        return self.__statep__

    @property
    def statepp(self):
        if '__statepp__' not in self.__dict__: self.__fill_states__()
        return self.__statepp__
        
    @property
    def par_line(self):
        rowobj = createRowObject(self)
        return putRowObjectToString(rowobj)

    def __str__(self):
        return self.par_line

    def __repr__(self):
        return self.__str__()        

@searchable
class IsotopologueAlias:

    __keys__ = (
        ('id',               {'type':int}),
        ('isotopologue_id',  {'type':int}),
        ('alias',            {'type':str}),
        ('type',             {'type':str}),
    )

    __identity__ = 'alias'
    
    #__refs__ = {}
    __refs__ = {
        'isotopologue': {
            'class':'Isotopologue',
            'table':'isotopologue',
            'join':[('isotopologue_id','id'),],
            'backpop':'aliases',
        },
    }

    __backrefs__ = {}

    @property
    def name(self):
        return self.alias

    @property
    def molecule(self):
        if self.isotopologue:
            return self.isotopologue.molecule
        else:
            return none

@searchable_by_alias
class Isotopologue:

    __keys__ =  (
        ('id',                 {'type':int}),
        ('molecule_alias_id',  {'type':int}),
        ('isoid',              {'type':int}),
        ('inchi',              {'type':str}),
        ('inchikey',           {'type':str}),
        ('iso_name',           {'type':str}),
        ('iso_name_html',      {'type':str}),
        ('abundance',          {'type':float}),
        ('mass',               {'type':float}),
        ('afgl_code',          {'type':str}),
    )

    __identity__ = 'iso_name'
    
    __refs__ = {
        'molecule_alias': {
            'class':'MoleculeAlias',
            'table':'molecule_alias',
            'join':[('molecule_alias_id','id'),],
            'backpop':'isotopologues',
        },
    }

    __backrefs__ = {
        'aliases': {
            'class':'IsotopologueAlias',
            'table':'isotopologue_alias',
            'join':[('id','isotopologue_id'),],
            'backpop':'isotopologue',
        },
    }

    #@property
    #def name(self):
    #    return self.iso_name

    def __set_name__(self,name):
        self.iso_name = name
        
    def __get_name__(self):
        return self.iso_name
        
    @property
    def molecule(self):
        return self.molecule_alias.molecule

    @property
    def transitions(self):
        raise NotImplementedError
        
    @property
    def partition_functions(self):
        pfuncs = set()
        for iso_alias in self.aliases:
            pfuncs.update(iso_alias.partition_functions)
        return list(pfuncs)

@searchable        
class MoleculeCategory:

    __keys__ = (
        ('id',        {'type':int}),
        ('category',  {'type':str}),
    )

    __identity__ = 'category'
    
    __refs__ = {}

    __backrefs__ = {}

    @property
    def molecule_aliases(self):
        raise NotImplementedError

    @property
    def molecules(self):
        mols = []
        for molecule_alias in self.molecule_aliases:
            mols.append(molecule_alias.molecule)
        return list(set(mols))

    @property
    def cross_sections(self):
        xss = set()
        for mol in self.molecules:
            xss.update(mol.cross_sections)
        return list(xss)

    @property
    def sources(self):
        srcs = set()
        for mol in self.molecules:
            srcs.update(mol.sources)
        return list(srcs)

@searchable              
class MoleculeAlias:

    __keys__ = (
        ('id',           {'type':int}),
        ('molecule_id',  {'type':int}),
        ('alias',        {'type':str}),
        ('type',         {'type':str}),
    )

    __identity__ = 'alias'
    
    #__refs__ = {}
    __refs__ = {
        'molecule': {
            'class':'Molecule',
            'table':'molecule',
            'join':[('molecule_id','id'),],
            'backpop':'aliases',
        },
    }

    __backrefs__ = {}

    @property
    def name(self):
        return self.alias

    @property
    def molecule(self):
        raise NotImplementedError

@searchable_by_alias        
class Molecule:

    __keys__ =  (
        ('id',                      {'type':int}),
        ('common_name',             {'type':str}),
        ('ordinary_formula',        {'type':str}),
        ('ordinary_formula_html',   {'type':str}),
        ('stoichiometric_formula',  {'type':str}),
        ('inchi',                   {'type':str}),
        ('inchikey',                {'type':str}),
    )

    __identity__ = 'common_name'
    
    __refs__ = {}

    __backrefs__ = {
        'aliases': {
            'class':'MoleculeAlias',
            'table':'molecule_alias',
            'join':[('id','molecule_id'),],
            'backpop':'molecule',
        },
    }

    #@property
    #def name(self):
    #    return self.common_name
        
    def __set_name__(self,name):
        self.common_name = name
        
    def __get_name__(self):
        return self.common_name

    @property
    def aliases(self):
        raise NotImplementedError

    @property
    def first_alias(self):
        raise NotImplementedError

    @staticmethod
    def first(molname=None):
        raise NotImplementedError

    @property
    def cross_sections(self):
        xss = set()
        for molecule_alias in self.aliases:
            xss.update(molecule_alias.cross_sections)
        return list(xss)

    @property
    def isotopologues(self):
        isos = set()
        for molecule_alias in self.aliases:
            isos.update(molecule_alias.isotopologues)
        return list(isos)

    @property
    def isotopologue_alias_ids(self):
        isoals = set()
        for iso in self.isotopologues:
            isoals.update(iso.aliases)
        return [isoal.id for isoal in isoals]

    @property
    def transitions(self):
        raise NotImplementedError

    @property
    def linelists(self):
        llists = set()
        for trans in self.transitions:
            llists.update(trans.linelists)
        return list(llists)

    @property
    def categories(self):
        cats = set()
        for molecule_alias in self.aliases:
            cats.update(molecule_alias.categories)
        return list(cats)

    @property
    def sources(self):
        srcs = []
        for xs in self.cross_sections:
            srcs.append(xs.source)
        return list(set(srcs))

    @property
    def groups(self):
        grps = []
        for src in self.sources:
            grps.append(src.group)
        return list(set(grps))

    @property
    def acronym(self):
        for al in self.aliases:
            if al.type=='acronym': return al

    @property
    def cas(self):
        for al in self.aliases:
            if al.type=='cas': return al

    @property
    def atoms(self):
        return atoms(self.stoichiometric_formula)

    @property
    def natoms(self):
        return natoms(self.stoichiometric_formula)

    @property
    def weight(self):
        return molweight(self.stoichiometric_formula)

@searchable              
class CollisionComplexAlias:

    __keys__ = (
        ('id',                    {'type':int}),
        ('collision_complex_id',  {'type':int}),
        ('alias',                 {'type':str}),
        ('type',                  {'type':str}),
    )

    __identity__ = 'alias'
    
    #__refs__ = {}
    __refs__ = {
        'collision_complex': {
            'class':'CollisionComplex',
            'table':'collision_complex',
            'join':[('collision_complex_id','id'),],
            'backpop':'aliases',
        },
    }

    __backrefs__ = {}

    @property
    def name(self):
        return self.alias

    @property
    def collision_complex(self):
        raise NotImplementedError

@searchable_by_alias        
class CollisionComplex:

    __keys__ =  (
        ('id',                      {'type':int}),
        ('chemical_symbol',         {'type':str}),
    )

    __identity__ = 'chemical_symbol'
    
    __refs__ = {}

    __backrefs__ = {
        'aliases': {
            'class':'CollisionComplexAlias',
            'table':'collision_complex_alias',
            'join':[('id','collision_complex_id'),],
            'backpop':'collision_complex',
        },
    }

    #@property
    #def name(self):
    #    return self.chemical_symbol
        
    def __set_name__(self,name):
        self.chemical_symbol = name
        
    def __get_name__(self):
        return self.chemical_symbol

    @property
    def aliases(self):
        raise NotImplementedError

    @property
    def first_alias(self):
        raise NotImplementedError

    @property
    def cia_cross_sections(self):
        xss = set()
        for ccomp_alias in self.aliases:
            xss.update(ccomp_alias.cia_cross_sections)
        return list(xss)

    @property
    def sources(self):
        srcs = []
        for xs in self.cia_cross_sections:
            srcs.append(xs.source)
        return list(set(srcs))

    @property
    def groups(self):
        grps = []
        for src in self.sources:
            grps.append(src.group)
        return list(set(grps))

    #@property
    #def acronym(self):
    #    for al in self.aliases:
    #        if al.type=='acronym': return al

    #@property
    #def cas(self):
    #    for al in self.aliases:
    #        if al.type=='cas': return al

    #@property
    #def atoms(self):
    #    return atoms(self.stoichiometric_formula)

    #@property
    #def natoms(self):
    #    return natoms(self.stoichiometric_formula)

    #@property
    #def weight(self):
    #    return molweight(self.stoichiometric_formula)
