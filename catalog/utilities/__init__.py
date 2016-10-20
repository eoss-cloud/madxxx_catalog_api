#-*- coding: utf-8 -*-

""" EOSS catalog system
utilities module

general function collection
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

import uuid

import os
from sqlalchemy.dialects.postgresql.base import UUID
from sqlalchemy.sql.sqltypes import CHAR
from sqlalchemy.sql.type_api import TypeDecorator


def read_OS_var(var_name, mandatory=True, default_val=None):
    """
    Read OS variable safely
    :param var_name:
    :param mandatory: raise Exception if variable does not exist - otherwise return None
    :param default_val: provide a default return value if OS variable does not exist
    :return: OS var content
    """
    if var_name in os.environ:
        return os.environ[var_name]
    else:
        if mandatory:
            raise Exception("OS var: %s does not exist. Please set it before executing the module." % var_name)
        if default_val != None:
            return default_val
        else:
            return None


class GUID(TypeDecorator):
    """Platform-independent GUID type.

    Uses Postgresql's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.

    """
    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % uuid.UUID(value).int
            else:
                # hexstring
                return "%.32x" % value.int

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return uuid.UUID(value)


def items(d):  # noqa
    return d.iteritems()


def with_metaclass(Type, skip_attrs=set(['__dict__', '__weakref__'])):
    """Class decorator to set metaclass. Used for Singelton/abstract class decoration

    Works with both Python 2 and Python 3 and it does not add
    an extra class in the lookup order like ``six.with_metaclass`` does
    (that is -- it copies the original class instead of using inheritance).

    """

    def _clone_with_metaclass(Class):
        attrs = dict((key, value) for key, value in items(vars(Class))
                     if key not in skip_attrs)
        return Type(Class.__name__, Class.__bases__, attrs)

    return _clone_with_metaclass


class Singleton(type):
    """
    Inherited objects are Singeltons and exists only once per python interpreter execution
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances.keys():
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]
