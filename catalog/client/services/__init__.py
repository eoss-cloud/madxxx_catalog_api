#-*- coding: utf-8 -*-

""" EOSS catalog system
 web service package
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"


def getKeysFromDict(dataDict, mapList):
    """
    Return value from a multiple key given by tuple representation e.g. (mainkey, subkey1, subkey2)
    :param dataDict: whole dictionary
    :param mapList: key name tuple
    :return: value of multiple key
    """
    return reduce(lambda my_dict, key: my_dict[key], mapList, dataDict)
