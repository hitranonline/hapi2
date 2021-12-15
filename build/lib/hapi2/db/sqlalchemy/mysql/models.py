""" Mappings for MySQL backend of SQLAlchemy """

from ..base import LONGBLOB, TEXT, String, DOUBLE, Integer, Date, Table

from ..base import declarative_base, Column, deferred, PickleType

from ..base import make_session_default

from ..base import commit, query

from hapi2.db.sqlalchemy import models

BLOBTYPE = LONGBLOB
TEXTTYPE = TEXT
VARCHARTYPE = String
DOUBLETYPE = DOUBLE(asdecimal=False)
INTTYPE = Integer
DATETYPE = Date

Base = declarative_base()
make_session = make_session_default
    
engine_meta = None
IS_UNIQUE = True
IS_NULLABLE = True

from hapi2.config import SETTINGS
table_engine = SETTINGS['table_engine']

def search_string(query,cls,field,pattern):
    return query.filter(getattr(cls,field).ilike(pattern))

class CrossSectionData(models.CrossSectionData, Base):

    id = Column(INTTYPE,primary_key=True)
    header_id = Column('header_id',INTTYPE,nullable=IS_NULLABLE) # ,ForeignKey('cross_section.id')
    __nu__ = Column('__nu__',BLOBTYPE)
    __coef__ = Column('__coef__',BLOBTYPE)

    __table_args__ = (
        {'mysql_engine':table_engine},
    )

class CrossSection(models.CrossSection, Base):

    id = Column(INTTYPE,primary_key=True,autoincrement=False)
    molecule_alias_id = Column('molecule_alias_id',INTTYPE,nullable=IS_NULLABLE) # ,ForeignKey('molecule_alias.id')
    source_alias_id = Column('source_alias_id',INTTYPE,nullable=IS_NULLABLE) # ,ForeignKey('source_alias.id')
    numin = Column('numin',DOUBLETYPE)
    numax = Column('numax',DOUBLETYPE)
    npnts = Column('npnts',INTTYPE)
    sigma_max = Column('sigma_max',DOUBLETYPE)
    temperature = Column('temperature',DOUBLETYPE)
    pressure = Column('pressure',DOUBLETYPE)
    resolution = Column('resolution',DOUBLETYPE)
    resolution_units = Column('resolution_units',VARCHARTYPE(5))
    broadener = Column('broadener',VARCHARTYPE(255))
    description = Column('description',VARCHARTYPE(255))
    apodization = Column('apodization',VARCHARTYPE(255))
    json = Column('json',VARCHARTYPE(255)) # auxiliary field containing non-schema information

    # additional HITRANonline-compliant parameters parameters
    filename = Column('filename',VARCHARTYPE(250),unique=IS_UNIQUE) # HITRANonline filename

    __table_args__ = (
        #Index('cross_section__molecule_alias_id', molecule_alias_id),
        {'mysql_engine':table_engine},
    )

class SourceAlias(models.SourceAlias, Base):

    id = Column(INTTYPE,primary_key=True,autoincrement=False)
    source_id = Column('source_id',INTTYPE,nullable=True) # ,ForeignKey('source.id')
    alias = Column('alias',VARCHARTYPE(250),unique=IS_UNIQUE,nullable=IS_NULLABLE)
    type = Column('type',VARCHARTYPE(255))

    __table_args__ = (
        {'mysql_engine':table_engine},
    )

class Source(models.Source, Base):

    id = Column(INTTYPE,primary_key=True,autoincrement=False)
    short_alias = Column('short_alias',VARCHARTYPE(250),nullable=IS_NULLABLE,unique=IS_UNIQUE)
    type = Column('type',VARCHARTYPE(255))
    authors = Column('authors',TEXTTYPE)
    title = Column('title',TEXTTYPE)
    journal = Column('journal',VARCHARTYPE(255))
    volume = Column('volume',VARCHARTYPE(255))
    page_start = Column('page_start',VARCHARTYPE(255))
    page_end = Column('page_end',VARCHARTYPE(255))
    year = Column('year',INTTYPE)
    institution = Column('institution',VARCHARTYPE(255))
    note = Column('note',TEXTTYPE)
    doi = Column('doi',VARCHARTYPE(255))
    bibcode = Column('bibcode',VARCHARTYPE(255))
    url = Column('url',TEXTTYPE)

    __table_args__ = (
        {'mysql_engine':table_engine},
    )

class ParameterMeta(models.ParameterMeta, Base):

    id = Column(INTTYPE, primary_key=True,autoincrement=False)
    name = Column('name',VARCHARTYPE(250),unique=IS_UNIQUE,nullable=IS_NULLABLE)
    type = Column('type',VARCHARTYPE(255))
    description = Column('description',VARCHARTYPE(255))
    format = Column('format',VARCHARTYPE(255))
    units = Column('units',VARCHARTYPE(255))

    __table_args__ = (
        {'mysql_engine':table_engine},
    )

linelist_vs_transition = Table('linelist_vs_transition', Base.metadata,
    Column('linelist_id', INTTYPE), #, ForeignKey('linelist.id')
    Column('transition_id', INTTYPE), #, ForeignKey('transition.id')
    #Index('linelist_vs_transition__linelist_id','linelist_id'), # fast search for transitions for given linelist
    mysql_engine=table_engine,
)

class Linelist(models.Linelist, Base):

    id = Column('id',INTTYPE,primary_key=True,autoincrement=False)
    name = Column('name',VARCHARTYPE(250),unique=IS_UNIQUE,nullable=False)
    description = Column('description',VARCHARTYPE(255))

    __table_args__ = (
        {'mysql_engine':table_engine},
    )

class Transition(models.Transition, Base):

    id = Column(INTTYPE,primary_key=True,autoincrement=False)
    isotopologue_alias_id = Column('isotopologue_alias_id',INTTYPE,nullable=IS_NULLABLE) #,ForeignKey('isotopologue_alias.id')
    molec_id = Column('molec_id',INTTYPE)
    local_iso_id = Column('local_iso_id',INTTYPE)
    nu = Column('nu',DOUBLETYPE)
    sw = Column('sw',DOUBLETYPE)
    a = Column('a',DOUBLETYPE)
    gamma_air = Column('gamma_air',DOUBLETYPE)
    gamma_self = Column('gamma_self',DOUBLETYPE)
    elower = Column('elower',DOUBLETYPE)
    n_air = Column('n_air',DOUBLETYPE)
    delta_air = Column('delta_air',DOUBLETYPE)
    global_upper_quanta = Column('global_upper_quanta',VARCHARTYPE(15))
    global_lower_quanta = Column('global_lower_quanta',VARCHARTYPE(15))
    local_upper_quanta = Column('local_upper_quanta',VARCHARTYPE(15))
    local_lower_quanta = Column('local_lower_quanta',VARCHARTYPE(15))
    ierr = Column('ierr',VARCHARTYPE(6))
    iref = Column('iref',VARCHARTYPE(12))
    line_mixing_flag = Column('line_mixing_flag',VARCHARTYPE(1))
    gp = Column('gp',INTTYPE)
    gpp = Column('gpp',INTTYPE)
    
    # Simplest solution possible: all "non-standard" parameters are stored 
    # in the main Transition table as a keys of a picklable dictionary.
    extra = deferred(Column('extra',PickleType,default={}))

    __table_args__ = (
        #Index('transition__isotopologue_alias_id', isotopologue_alias_id),
        {'mysql_engine':table_engine},
    )

class IsotopologueAlias(models.IsotopologueAlias, Base):

    id = Column(INTTYPE,primary_key=True,autoincrement=False)
    isotopologue_id = Column('isotopologue_id',INTTYPE) # , ForeignKey('molecule.id')
    alias = Column('alias',VARCHARTYPE(250),unique=IS_UNIQUE,nullable=IS_NULLABLE)
    type = Column('type',VARCHARTYPE(255))

    __table_args__ = (
        {'mysql_engine':table_engine},
    )

class Isotopologue(models.Isotopologue, Base):
    
    id = Column(INTTYPE,primary_key=True,autoincrement=False)
    molecule_alias_id = Column('molecule_alias_id',INTTYPE,nullable=IS_NULLABLE) #,ForeignKey('molecule_alias.id')
    isoid = Column('isoid',INTTYPE)
    inchi = Column('inchi',VARCHARTYPE(250), unique=IS_UNIQUE)
    inchikey = Column('inchikey',VARCHARTYPE(250), unique=IS_UNIQUE)
    iso_name = Column('iso_name',VARCHARTYPE(250), unique=IS_UNIQUE)
    iso_name_html = Column('iso_name_html',VARCHARTYPE(255))
    abundance = Column('abundance',DOUBLETYPE, nullable=True)
    mass = Column('mass',DOUBLETYPE)
    afgl_code = Column('afgl_code',VARCHARTYPE(255))

    __table_args__ = (
        #Index('isotopologue__molecule_alias_id', molecule_alias_id),
        {'mysql_engine':table_engine},
    )

molecule_alias_vs_molecule_category = Table('molecule_alias_vs_molecule_category', Base.metadata,
    Column('molecule_alias_id', INTTYPE), #, ForeignKey('molecule_alias.id')
    Column('molecule_category_id', INTTYPE), #, ForeignKey('molecule_category.id')
    #Index('molecule_alias_vs_molecule_category__molecule_alias_id','molecule_alias_id'), # fast search for molecule aliases for given category
    mysql_engine=table_engine,
)      

class MoleculeCategory(models.MoleculeCategory, Base):
    
    id = Column(INTTYPE,primary_key=True,autoincrement=False)
    category = Column('category',VARCHARTYPE(250),unique=IS_UNIQUE,nullable=IS_NULLABLE)

    __table_args__ = (
        {'mysql_engine':table_engine},
    )

class MoleculeAlias(models.MoleculeAlias,Base):
    
    id = Column(INTTYPE, primary_key=True,autoincrement=False)
    molecule_id = Column('molecule_id',INTTYPE,nullable=True) # , ForeignKey('molecule.id')
    alias = Column('alias',VARCHARTYPE(250),unique=IS_UNIQUE,nullable=IS_NULLABLE)
    type = Column('type',VARCHARTYPE(255))

    __table_args__ = (
        {'mysql_engine':table_engine},
    )
    
class Molecule(models.Molecule, Base):
    
    id = Column(INTTYPE,primary_key=True,autoincrement=False)
    common_name = Column('common_name',VARCHARTYPE(255))
    ordinary_formula = Column('ordinary_formula',VARCHARTYPE(255))
    ordinary_formula_html = Column('ordinary_formula_html',VARCHARTYPE(255))
    stoichiometric_formula = Column('stoichiometric_formula',VARCHARTYPE(255))
    inchi = Column('inchi',VARCHARTYPE(255))
    inchikey = Column('inchikey',VARCHARTYPE(250),unique=IS_UNIQUE)
    csid = Column('csid',INTTYPE)

    __table_args__ = (
        {'mysql_engine':table_engine},
    )
