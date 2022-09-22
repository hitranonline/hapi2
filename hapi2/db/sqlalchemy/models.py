from functools import reduce

from .base import relationship, declared_attr, query

from hapi2.db import models
from hapi2.db.models import get_alias_class

from hapi2.config import VARSPACE, SETTINGS

from hapi2.format.dispatch import FormatDispatcher

from .updaters import __update_and_commit_core__
from .updaters import __insert_transitions_core__

from sqlalchemy import func, distinct

def searchable__alias(Cls):
    """
    Decorator for aliases (special case).
    """
    @classmethod        
    def get(cls,name=None):
        cls_ = getattr(VARSPACE['db_backend'].models,cls.__name__)
        search_string = VARSPACE['db_backend'].models.search_string
        if name is None: name = '%'
        return search_string(query(cls_),cls_,'alias',name).first()
        
    Cls.get = get
    
    return Cls

def searchable__name(Cls):
    """
    Decorator for "named" classes.
    """
    @classmethod        
    def get(cls,name=None):
        cls_ = getattr(VARSPACE['db_backend'].models,cls.__name__)
        search_string = VARSPACE['db_backend'].models.search_string
        if name is None: name = '%'
        return search_string(query(cls_),cls_,'name',name).first()
        
    Cls.get = get
    
    return Cls

def searchable_by_alias(Cls):
    """
    Decorator for "aliased" classes.
    """
    @classmethod
    def get(cls,name=None):
        cls_ = getattr(VARSPACE['db_backend'].models,cls.__name__)
        alias_cls_ = getattr(VARSPACE['db_backend'].models,
            cls.__backrefs__['aliases']['class'])
        search_string = VARSPACE['db_backend'].models.search_string
        if name is None: name = '%'
        return search_string(join_with_alias(cls_),alias_cls_,'alias',name).first()

    Cls.get = get
    
    return Cls

def join_with_alias(cls):
    """
    Input class must be a proper aliased class.
    Return SQLAlchemy join construct.
    """
    rname = 'aliases'
    refs = cls.__backrefs__
    
    alias_cls = get_alias_class(cls)
    
    classname = refs[rname]['class']
        
    join_condition = []
    for i1,i2 in refs[rname]['join']:
        i1 = getattr(cls,i1)
        i2 = getattr(alias_cls,i2)
        join_condition.append(i1==i2)
    join_condition = reduce(lambda x,y:x and y,join_condition)
    
    q = query(cls).join(alias_cls,join_condition)
    
    return q
    
def assemble_relation(cls,rname,refs_flag):
    """
    Assemble SQLAlchemy relation construct from the higher-level markup.
    
    Example of the markup (for Isotopologue):
    
    __refs__ = {
        'molecule_alias': {
            'class':'MoleculeAlias',
            'table':'molecule_alias',
            'join':[('molecule_alias_id','id'),],
            'backpop':'isotopologues',
        },
    }    
    ==> Will result to the following SQLAlchemy construct:
    relationship('MoleculeAlias',back_populates='isotopologues',
        primaryjoin='foreign(isotopologue.c.molecule_alias_id)==molecule_alias.c.id')

    __backrefs__ = {
        'aliases': {
            'class':'IsotopologueAlias',
            'table':'isotopoloue_alias',
            'join':[('id','isotopologue_id'),],
            'backpop':'isotopologue',
        },
    }        
    ==> Will result to the following SQLAlchemy construct:    
    relationship('IsotopologueAlias',back_populates='isotopologue',
        primaryjoin='isotopologue.c.id==foreign(isotopologue_alias.c.isotopologue_id)')
    
    The argument "rname" defines the name of the relation.

    The argument "rafs_flag" is True is refs are considered, 
    otherwise False if backrefs.
    
    """    
    if refs_flag:
        refs = cls.__refs__
    else:
        refs = cls.__backrefs__
    
    classname = refs[rname]['class']
    backpop = refs[rname]['backpop']
        
    joinstrings = []
    for i1,i2 in refs[rname]['join']:
        i1 = '%s.c.%s'%(cls.__tablename__,i1)
        i2 = '%s.c.%s'%(refs[rname]['table'],i2)
        if refs_flag:
            i1 = 'foreign(%s)'%i1
        else:
            i2 = 'foreign(%s)'%i2
        joinstrings.append('%s==%s'%(i1,i2))
    joinstr = ' and '.join(joinstrings)
    
    rel = relationship(classname,
        back_populates=backpop,primaryjoin=joinstr)
        
    return rel

def find_local_id(obj):
    """ Find unoccupied local id """
    id_ = query(func.min(obj.__class__.id)).first()[0]
    if id_ is None: 
        id_ = 0
    return id_ - 1

def save_local_recursive(obj): # recursively save all local objects with negative ids
    if obj.id is None:
        obj.id = find_local_id(obj)
    for refname in obj.__refs__:
        ref_obj = getattr(obj,refname)
        if ref_obj is not None:
            save_local_recursive(ref_obj)
        ref_obj_id_field = obj.__refs__[refname]['join'][0][0]
        setattr(obj,ref_obj_id_field,ref_obj.id)
    VARSPACE['session'].add(obj)

class CRUD_Generic(models.CRUD):    

    """
    Generic class implementing CRUD feature subsets of the database.
    Default behaviour based on the assumption that the contents of the 
    HAPI headers are given in plain JSON format.
    
    To override the default behaviour (like for Transition class)
    create a child class redefining the "update" method and "__format_dispatcher_class__"
    class field.
    """
    
    __format_dispatcher_class__ = FormatDispatcher
        
    @classmethod
    def update(cls,header,local=True,**argv):
        tmpdir = SETTINGS['tmpdir']
        cls.__check_types__(header)                   
        stream = cls.__format_dispatcher_class__().getStreamer(basedir=tmpdir,header=header)
        return __update_and_commit_core__(
            cls,stream,cls.__refs__,cls.__backrefs__,local=local,**argv)

    @classmethod
    def read(cls,filter=None,colnames=None):
        session = VARSPACE['session']
        model = getattr(VARSPACE['db_backend'].models,cls.__name__)
                
        q = session.query(model)
        if filter is not None:
            if type(filter) not in [list,tuple]:
                filter = [filter]
            q = q.filter(*[filter])
                    
        data = q.with_entities(
            *[getattr(cls,colname) for colname in colnames]
        ).all()
    
        return list(zip(*data))
                        
    @classmethod
    def load(cls,dct):
        session = VARSPACE['session']
        keys = [key for key,_ in cls.__keys__]
        keys_valid = set(keys).intersection(dct.keys())
        if 'id' in dct and dct['id'] is not None:
            obj = session.query(cls).filter(cls.id==dct['id']).first()
        else:
            obj = None
        if obj is None:
            obj = cls.construct(dct)
            session.expunge(obj)
        for key in keys_valid: 
            setattr(obj,key,dct[key])      
        return obj
                                      
    def __lt__(self,obj):
        return id(self) < id(obj)
        
    def save(self):
        if 'hash' in self.__dict__: # stub, must be removed when all objects will have hash
            _, hashval = self.__class__.pack(self)
            self.hash = hashval
        save_local_recursive(self)
        VARSPACE['session'].commit() # if no commit is done, fails on saving aliases


    @classmethod
    def all(cls):
        return VARSPACE['session'].query(cls).all()

class CRUD_Dotpar(CRUD_Generic):
    
    """
    Extension of the CRUD_Generic class 
    redefining fields and methods for Transition.
    """
    
    @classmethod
    def update(cls,header,local=True,llst_name='default',**argv):
        tmpdir = SETTINGS['tmpdir'] 
        cls.__check_types__(header)                   
        stream = cls.__format_dispatcher_class__().getStreamer(basedir=tmpdir,header=header)
        return __insert_transitions_core__(cls,stream,local=local,llst_name=llst_name,**argv)

class PartitionFunction(models.PartitionFunction):

    @declared_attr
    def __tablename__(cls):
        return 'partition_function'

    @declared_attr
    def isotopologue_alias(cls):
        return assemble_relation(cls,'isotopologue_alias',refs_flag=True)

    @declared_attr
    def source_alias(cls):
        return assemble_relation(cls,'source_alias',refs_flag=True)

class CrossSectionData(models.CrossSectionData):

    @declared_attr
    def __tablename__(src):
        return 'cross_section_data'

    @declared_attr
    def header(cls):
        return relationship('CrossSection',back_populates='data',
            primaryjoin='cross_section.c.id==foreign(cross_section_data.c.header_id)')

class CrossSection(models.CrossSection):

    @declared_attr
    def __tablename__(cls):
        return 'cross_section'

    @declared_attr
    def molecule_alias(cls):
        return assemble_relation(cls,'molecule_alias',refs_flag=True)

    @declared_attr
    def source_alias(cls):
        return assemble_relation(cls,'source_alias',refs_flag=True)

    @declared_attr
    def data(cls):
        return relationship('CrossSectionData',uselist=False,back_populates='header',
            primaryjoin = 'cross_section.c.id==foreign(cross_section_data.c.header_id)')
                
class CIACrossSectionData(models.CrossSectionData):

    @declared_attr
    def __tablename__(src):
        return 'cia_cross_section_data'

    @declared_attr
    def header(cls):
        return relationship('CIACrossSection',back_populates='data',
            primaryjoin='cia_cross_section.c.id==foreign(cia_cross_section_data.c.header_id)')

class CIACrossSection(models.CIACrossSection):

    @declared_attr
    def __tablename__(cls):
        return 'cia_cross_section'

    @declared_attr
    def collision_complex_alias(cls):
        return assemble_relation(cls,'collision_complex_alias',refs_flag=True)

    @declared_attr
    def source_alias(cls):
        return assemble_relation(cls,'source_alias',refs_flag=True)

    @declared_attr
    def data(cls):
        return relationship('CIACrossSectionData',uselist=False,back_populates='header',
            primaryjoin = 'cia_cross_section.c.id==foreign(cia_cross_section_data.c.header_id)')

@searchable__alias
class SourceAlias(models.SourceAlias):

    @declared_attr
    def __tablename__(cls):
        return 'source_alias'

    @declared_attr
    def source(cls):
        return relationship('Source',back_populates='aliases',
            primaryjoin='source.c.id==foreign(source_alias.c.source_id)')

    @declared_attr
    def cross_sections(cls):
        return relationship('CrossSection',back_populates='source_alias',
            primaryjoin='source_alias.c.id==foreign(cross_section.c.source_alias_id)')

    @declared_attr
    def cia_cross_sections(cls):
        return relationship('CIACrossSection',back_populates='source_alias',
            primaryjoin='source_alias.c.id==foreign(cia_cross_section.c.source_alias_id)')

    @declared_attr
    def partition_functions(cls):
        return relationship('PartitionFunction',back_populates='source_alias',
            primaryjoin='source_alias.c.id==foreign(partition_function.c.source_alias_id)')

@searchable_by_alias
class Source(models.Source):
    
    @declared_attr
    def __tablename__(cls):
        return 'source'

    @declared_attr
    def aliases(cls):
        return assemble_relation(cls,'aliases',refs_flag=False)

    @property
    def first_alias(self):
        if '__first_alias__' in self.__dict__:
            return self.__first_alias__
        else:
            return self.aliases[0]

    @staticmethod
    def first(srcname=None):
        models = VARSPACE['db_backend'].models
        if srcname is None: srcname = '%'
        return query(models.Source).join(models.SourceAlias,
            models.Source.id==models.SourceAlias.source_id).\
            filter(models.SourceAlias.alias.ilike(srcname+'\0%')).first()

@searchable__name
class ParameterMeta(models.ParameterMeta):

    __identity__ = 'name'

    @declared_attr
    def __tablename__(cls):
        return 'parameter_meta'

@searchable__name
class Linelist(models.Linelist):

    @declared_attr
    def __tablename__(cls):
        return 'linelist'

    @declared_attr
    def transitions(cls):
        return relationship('Transition',secondary='linelist_vs_transition',lazy='dynamic',
            primaryjoin='linelist.c.id==foreign(linelist_vs_transition.c.linelist_id)',
            secondaryjoin='transition.c.id==foreign(linelist_vs_transition.c.transition_id)')
    
    @property
    def isotopologues(self):
        
        models = VARSPACE['db_backend'].models        
              
        # get isotopologue alias ids
        isoal_ids = query(distinct(models.Transition.isotopologue_alias_id)).\
            join(
                models.linelist_vs_transition,
                models.linelist_vs_transition.c.transition_id==models.Transition.id
            ).\
            join(
                models.Linelist,
                models.linelist_vs_transition.c.linelist_id==models.Linelist.id
            ).all()
        isoal_ids = [c[0] for c in isoal_ids]
        
        # get isotopologue aliases and isotopologue ids
        isoals = query(models.IsotopologueAlias).\
            filter(
                models.IsotopologueAlias.id.in_(isoal_ids)
                )
        iso_ids = set([isoal.isotopologue_id for isoal in isoals])
        
        # get isotopologues and molecule alias ids
        isos = query(models.Isotopologue).\
            filter(
                models.Isotopologue.id.in_(iso_ids)
                )
                
        return isos.all()

    @property
    def molecules(self):
        
        models = VARSPACE['db_backend'].models 
        
        isos = self.isotopologues
        
        molal_ids = [iso.molecule_alias_id for iso in isos]
        
        # get molecule aliases and molecule ids
        molals = query(models.MoleculeAlias).\
            filter(
                models.MoleculeAlias.id.in_(molal_ids)
                )
        mol_ids = [molal.molecule_id for molal in molals]        
        
        # get molecules from the id list
        mols = query(models.Molecule).\
            filter(
                models.Molecule.id.in_(mol_ids)
                )
                
        return mols.all()

class Transition(models.Transition):

    @declared_attr
    def __tablename__(cls):
        return 'transition'
        
    @declared_attr
    def isotopologue_alias(cls):
        return assemble_relation(cls,'isotopologue_alias',refs_flag=True)

    @declared_attr
    def linelists(cls):
        return relationship('Linelist',secondary='linelist_vs_transition',
            primaryjoin='transition.c.id==foreign(linelist_vs_transition.c.transition_id)',
            secondaryjoin='linelist.c.id==foreign(linelist_vs_transition.c.linelist_id)',
            overlaps="transitions")

@searchable__alias
class IsotopologueAlias(models.IsotopologueAlias):

    @declared_attr
    def __tablename__(cls):
        return 'isotopologue_alias'

    @declared_attr
    def isotopologue(cls):
        return relationship('Isotopologue',back_populates='aliases',
            primaryjoin='foreign(isotopologue_alias.c.isotopologue_id)==isotopologue.c.id')

    @declared_attr
    def transitions(cls):
        return relationship('Transition',back_populates='isotopologue_alias',lazy='dynamic',
            primaryjoin='isotopologue_alias.c.id==foreign(transition.c.isotopologue_alias_id)')

    @declared_attr
    def partition_functions(cls):
        return relationship('PartitionFunction',back_populates='isotopologue_alias',lazy='dynamic',
            primaryjoin='isotopologue_alias.c.id==foreign(partition_function.c.isotopologue_alias_id)')

@searchable_by_alias
class Isotopologue(models.Isotopologue):

    @declared_attr
    def __tablename__(cls):
        return 'isotopologue'    

    @declared_attr
    def molecule_alias(cls):
        return assemble_relation(cls,'molecule_alias',refs_flag=True)

    @declared_attr
    def aliases(cls):
        return assemble_relation(cls,'aliases',refs_flag=False)

    @property
    def transitions(self):
        
        models = VARSPACE['db_backend'].models
        
        # get isotopologue aliases
        isoals = query(models.IsotopologueAlias).\
            join(
                models.Isotopologue,
                models.IsotopologueAlias.isotopologue_id==models.Isotopologue.id
                ).\
            filter(
                models.Isotopologue.id==self.id
                )
        isoal_ids = [isoal.id for isoal in isoals]
        
        # get transitions with such isoal ids
        transs = query(models.Transition).\
            filter(
                models.Transition.isotopologue_alias_id.in_(isoal_ids)
                )
        
        return transs
        
class MoleculeCategory(models.MoleculeCategory):
    
    @declared_attr 
    def __tablename__(cls):
        return 'molecule_category'

    @declared_attr 
    def molecule_aliases(cls):
        return relationship('MoleculeAlias',secondary='molecule_alias_vs_molecule_category',
            primaryjoin='molecule_category.c.id==foreign(molecule_alias_vs_molecule_category.c.molecule_category_id)',
            secondaryjoin='molecule_alias.c.id==foreign(molecule_alias_vs_molecule_category.c.molecule_alias_id)')

@searchable__alias
class MoleculeAlias(models.MoleculeAlias):

    @declared_attr
    def __tablename__(cls):
        return 'molecule_alias'

    @declared_attr
    def molecule(cls):
        return relationship('Molecule',back_populates='aliases',
            primaryjoin='foreign(molecule_alias.c.molecule_id)==molecule.c.id')
            
    @declared_attr
    def cross_sections(cls):
        return relationship('CrossSection',back_populates='molecule_alias',
            primaryjoin='molecule_alias.c.id==foreign(cross_section.c.molecule_alias_id)')

    @declared_attr
    def isotopologues(cls):
        return relationship('Isotopologue',back_populates='molecule_alias',
            primaryjoin='molecule_alias.c.id==foreign(isotopologue.c.molecule_alias_id)')

    @declared_attr
    def categories(cls):
        return relationship('MoleculeCategory',secondary='molecule_alias_vs_molecule_category',
            primaryjoin='molecule_alias.c.id==foreign(molecule_alias_vs_molecule_category.c.molecule_alias_id)',
            secondaryjoin='molecule_category.c.id==foreign(molecule_alias_vs_molecule_category.c.molecule_category_id)',
            overlaps="molecule_aliases")

@searchable_by_alias
class Molecule(models.Molecule):

    @declared_attr
    def __tablename__(cls):
        return 'molecule'
        
    @declared_attr
    def aliases(cls):
        return assemble_relation(cls,'aliases',refs_flag=False)

    @property
    def transitions(self):
        
        models = VARSPACE['db_backend'].models        
        
        # get isotopologue aliases
        isoals = query(models.IsotopologueAlias).\
            join(
                models.Isotopologue,
                models.IsotopologueAlias.isotopologue_id==models.Isotopologue.id
                ).\
            join(
                models.MoleculeAlias,
                models.Isotopologue.molecule_alias_id==models.MoleculeAlias.id).\
            filter(
                models.MoleculeAlias.molecule_id==self.id
                )
        isoal_ids = [isoal.id for isoal in isoals]
        
        # get transitions with such isoal ids
        transs = query(models.Transition).filter(
            models.Transition.isotopologue_alias_id.in_(isoal_ids))
        
        return transs

@searchable__alias
class CollisionComplexAlias(models.CollisionComplexAlias):

    @declared_attr
    def __tablename__(cls):
        return 'collision_complex_alias'

    @declared_attr
    def collision_complex(cls):
        return relationship('CollisionComplex',back_populates='aliases',
            primaryjoin='foreign(collision_complex_alias.c.collision_complex_id)==collision_complex.c.id')
            
    @declared_attr
    def cia_cross_sections(cls):
        return relationship('CIACrossSection',back_populates='collision_complex_alias',
            primaryjoin='collision_complex_alias.c.id==foreign(cia_cross_section.c.collision_complex_alias_id)')

@searchable_by_alias
class CollisionComplex(models.CollisionComplex):

    @declared_attr
    def __tablename__(cls):
        return 'collision_complex'
        
    @declared_attr
    def aliases(cls):
        return assemble_relation(cls,'aliases',refs_flag=False)
