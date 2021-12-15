import os
import sys
import json
from datetime import datetime, date
from dateutil import parser as dateutil_parser
from sqlalchemy.sql.sqltypes import Date as SQLAlchemyDate
from getpass import getpass
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm.collections import InstrumentedList

from sqlalchemy import Table,Column,String,Integer,Boolean,Float,Date,DateTime,BLOB,PickleType,ForeignKey

from sqlalchemy import Index

from sqlalchemy.dialects.mysql import LONGBLOB,TEXT,DOUBLE
from sqlalchemy.orm import relationship
from sqlalchemy.orm import deferred
from sqlalchemy import inspect
from sqlalchemy import sql
from sqlalchemy.sql.expression import func
from sqlalchemy.sql.expression import bindparam

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr

from sqlalchemy.orm import sessionmaker

from sqlalchemy.schema import PrimaryKeyConstraint, ForeignKeyConstraint

from sqlalchemy import func

from sqlalchemy.sql import text as text_

#from ..collect import jeanny3

from hapi2.config import VARSPACE

def make_session_default(engine):
    __Session__ = sessionmaker(bind=engine,autoflush=False)
    __session__ = __Session__()
    return __session__

def commit():
    VARSPACE['session'].commit()

def query(*args):
    return VARSPACE['session'].query(*args)
