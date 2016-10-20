#-*- coding: utf-8 -*-

""" EOSS catalog system
 Custom exceptions used in the catalog application
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"


class ApiException(Exception):
    """
    Exception used in the API layer
    """
    pass


class SerializerException(Exception):
    """
    Exception used in the serialization/deserialization layer
    """
    pass