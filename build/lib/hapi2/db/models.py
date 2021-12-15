from hapi2.config import VARSPACE        
from hapi2.utils.formula import molweight,atoms,natoms
from hapi import putRowObjectToString,HITRAN_DEFAULT_HEADER

def searchable(Cls):
    """
    Decorator for aliases.
    """    
    @classmethod
    def get(cls,name=None):
        cls_ = getattr(VARSPACE['db_backend'].models,cls.__name__)
        return cls_.get(name)

    def __init__(self,name=None):
        if self in VARSPACE['session']:
            return
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
    def get(cls,name=None):
        cls_ = getattr(VARSPACE['db_backend'].models,cls.__name__)
        return cls_.get(name)

    def __init__(self,name=None):
        if self in VARSPACE['session']:
            return
        if name is not None:
            #self.iso_name = name
            self.name = name
            al = get_alias_class(self.__class__)(name); al.type = 'generic'
            self.aliases.append(al)

    def __new__(cls,name=None):
        if name is None:
            return super(Cls, cls).__new__(cls)
        obj = Cls.get(name)
        if obj is None:
            return super(Cls, cls).__new__(cls)
        else:
            return obj
            
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

class CRUD:
    """
    Implements helper funcs for (C)reate, (R)ead, (U)pdate, and (D)elete.
    """
    
    @classmethod
    def update(cls,dct):
        """
        Update existing entry or create a new one in case 
        if the latter is absent.
        """
        raise NotImplementedError
        
    def dump(self):
        """
        Dump serialized keys given in __keys__
        field to the dictionary object.
        """
        raise NotImplementedError
        
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

class CrossSectionData:
    """
    Stores the actual data for the header given in CrossSection.
    Wave numbers are packed in 8-byte double precision,
    while absorption coefficient is packed in 4-byte single precision.
    Single precision for absorption coefficient gives around 5e-6 percent
    difference in accuracy, but reduces the database size twice.
    For wave numbers, single precision generally is not enough.
    """

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

    def unpack_coef(self):
        data_coef = self.__coef__
        if not data_coef:
            print('coef is empty')
            return None
        else:
            return np.array(unpack_float(decompress_zlib(data_coef)))

    def pack_nu(self,nu):
        self.__nu__ = compress_zlib(pack_double(nu))

    def pack_coef(self,coef):
        self.__coef__ = compress_zlib(pack_float(coef))

class CrossSection:

    __keys__ = (
        'id',
        'molecule_alias_id',
        'source_alias_id',
        'numin',
        'numax',
        'npnts',
        'sigma_max',
        'temperature',
        'pressure',
        'resolution',
        'resolution_units',
        'broadener',
        'description',
        'apodization',
        'json',
        'filename',
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

    @property
    def molecule(self):
        return self.molecule_alias.molecule

    @property
    def source(self):
        return self.source_alias.source

    def set_data(self,nu,coef):
        if coef is None:
            raise Exception('coef must be non-empty')
        if nu is None:
            if not (self.numin or self.numax):
                raise Exception('numin and numax must be non-empty')
            self.npnts = len(coef)
        elif len(nu)!=len(coef):
            raise Exception('nu and coef must have the same length')
        self.data = CrossSectionData()
        if nu is not None:
            #self.data.nu = nu # attribute version works badly
            self.data.pack_nu(nu)
            self.numin = min(nu)
            self.numax = max(nu)
        #self.data.coef = coef # attribute version works badly
        self.data.pack_coef(coef)
        #self.data.header = self # back link (not necessary when on-to-one relationship is properly set up)

    def get_data(self):
        """
        Either get the spectral data from the "data" relation,
        or try to open the file in case if "data" is empty.
        The file is defined by three parameters:
           1) srcdir -> file directory
           2) srcfile -> file name
           3) srcformat -> file format ('twocol','xsc-hit')
        """
        # Search for the data blob first.
        if self.data:
            #return self.data.get_nu,self.data.coef # attribute version works badly
            return self.data.unpack_nu(),self.data.unpack_coef()
        # If not found, try to import the file.
        if self.srcformat is None:
            raise Exception('srcformat should not be empty')
        if self.srcformat=='2col':
            path = os.path.join(self.srcdir,self.srcfile)
            if not os.path.isfile(path): # search for gzipped file with single extension
                path,ext = os.path.splitext(path); path += '.gz'
            if not os.path.isfile(path): # search for gzipped file with double extension
                path,_ = os.path.splitext(path); path += ext + '.gz'
            #pairs = np.loadtxt(path) # UNICODE BUG
            pairs = np.loadtxt(str(path))
            nu,coef = zip(*pairs)
            return np.array(nu),np.array(coef)
        elif self.srcformat=='xsc':
            coef,HEADER = read_xsc(self.srcdir,self.srcfile)
            nu = np.linspace(HEADER['numin'],HEADER['numax'],HEADER['npnts'])
            return nu,coef
        else:
            raise Exception('unknown value for srcformat: %s'%self.srcformat)

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
        nu,coef = self.range(numin,numax)
        binned_nu,binned_coef = downsample(nu,coef,delta,type)
        return binned_nu,binned_coef

    def compare(self,xs,numin,numax,grid=None):
        raise NotImplementedError

    def __str__(self):
        return '%s : %s'%(self.source_alias,self.molecule_alias)
    
    def __repr__(self):
        return self.__str__()


@searchable
class SourceAlias:

    __keys__ = (
        'id',
        'source_id',
        'alias',
        'type'
    )
        
    __identity__ = 'alias'

    __refs__ = {}

    __backrefs__ = {}

    @property
    def name(self):
        return self.alias

@searchable_by_alias
class Source:

    __keys__ = ('id','type','authors','title',
                'journal','volume','page_start','page_end',
                'year','institution','note','doi','bibcode',
                'url','short_alias')

    __identity__ = 'id'
    
    __refs__ = {}

    __backrefs__ = {
        'aliases': {
            'class':'SourceAlias',
            'table':'source_alias',
            'join':[('id','source_id'),],
            'backpop':'source',
        },
    }

    @property
    def name(self):
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
        'id',
        'name',
        'type',
        'description',
        'format',
        'units',
    )

    __identity__ = 'name'
    
    __refs__ = {}

    __backrefs__ = {}

@searchable
class Linelist:
  
    __keys__ = (
        'id',
        'name',
        'description',
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
        'id',
        'isotopologue_alias_id',
        'molec_id',
        'local_iso_id',
        'nu',
        'sw',
        'a',
        'gamma_air',
        'gamma_self',
        'elower',
        'n_air',
        'delta_air',
        'global_upper_quanta',
        'global_lower_quanta',
        'local_upper_quanta',
        'local_lower_quanta',
        'ierr',
        'iref',
        'line_mixing_flag',
        'gp',
        'gpp',
        'extra',
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
        'id',
        'isotopologue_id',
        'alias',
        'type',
    )

    __identity__ = 'alias'
    
    __refs__ = {}

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
        'id',
        'isoid',
        'inchi',
        'inchikey',
        'iso_name',
        'iso_name_html',
        'abundance',
        'mass',
        'afgl_code',
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

    @property
    def name(self):
        return self.iso_name
        
    @property
    def molecule(self):
        return self.molecule_alias.molecule

    @property
    def transitions(self):
        raise NotImplementedError

@searchable        
class MoleculeCategory:

    __keys__ = (
        'id',
        'category',
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
        'id',
        'molecule_id',
        'alias',
        'type',
    )

    __identity__ = 'alias'
    
    __refs__ = {}

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
        'id',
        'common_name',
        'ordinary_formula',
        'ordinary_formula_html',
        'stoichiometric_formula',
        'inchi',
        'inchikey'
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

    @property
    def name(self):
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
