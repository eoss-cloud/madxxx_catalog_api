#-*- coding: utf-8 -*-

""" EOSS catalog system
external catalog management package
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

from abc import ABCMeta, abstractmethod

from utilities import with_metaclass


@with_metaclass(ABCMeta)
class ICatalog(object):
    """
    Simple catalog interface class
    """
    def __init__(self):
        pass

    @abstractmethod
    def find(self):
        pass

    @abstractmethod
    def register(self, ds):
        pass
