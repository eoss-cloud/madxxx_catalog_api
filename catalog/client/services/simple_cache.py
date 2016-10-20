#-*- coding: utf-8 -*-

""" EOSS catalog system
 Simple dictionary based Cache backend for dogpile
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

import logging

from dogpile.cache import make_region
from dogpile.cache import register_backend
from dogpile.cache.api import CacheBackend, NO_VALUE
from dogpile.cache.proxy import ProxyBackend

logger = logging.getLogger(__name__)


class LoggingProxy(ProxyBackend):
    def set(self, key, value):
        logger.debug('Setting Cache Key: %s' % key)
        self.proxied.set(key, value)


class DictionaryBackend(CacheBackend):
    def __init__(self, arguments):
        self.cache = {}

    def get(self, key):
        return self.cache.get(key, NO_VALUE)

    def set(self, key, value):
        self.cache[key] = value

    def delete(self, key):
        self.cache.pop(key)


def prepare_cache():
    register_backend("dictionary", "client.services.simple_cache", "DictionaryBackend")

    region = make_region("myregion")
    region.configure("dictionary", wrap=[LoggingProxy])

    return region


region = prepare_cache()
