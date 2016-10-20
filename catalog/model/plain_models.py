#-*- coding: utf-8 -*-

""" EOSS catalog system
catalog objects and fixed data structures used for the serialization/deserialization process
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

from datetime import datetime


class ResourcesURLS(object):
    def __init__(self):
        self.metadata_url = None
        self.resource_url = None
        self.quicklook_url = None


class Catalog_Dataset(object):
    def __init__(self):
        self.entity_id = None
        self.acq_time = None
        self.sensor = None
        self.tile_identifier = None
        self.clouds = None
        self.level = None
        self.daynight = None
        self.time_registered = datetime.utcnow()

    def __hash__(self):
        return hash(self.entity_id) ^ hash(self.tile_identifier) ^ hash(self.acq_time)


class S3PrivateContainer(object):
    def __init__(self):
        self.region = None
        self.bucket = None
        self.filename = None

    def to_dict(self):
        return dict(s3privat=self.__dict__)


class S3PublicContainer(object):
    def __init__(self):
        self.http = None
        self.bucket = None
        self.prefix = None

    def to_dict(self):
        return dict(s3public=self.__dict__)


class SentinelS3Container(object):
    def __init__(self):
        self.zip = None
        self.bucket = None
        self.tile = None
        self.product = None
        self.quicklook = None

    def to_dict(self):
        return dict(s3public=self.__dict__)


class CopernicusSciHubContainer(object):
    def __init__(self):
        self.http = None

    def to_dict(self):
        return dict(scihub=self.__dict__)


class USGSOrderContainer(object):
    def __init__(self):
        self.link = None

    def to_dict(self):
        return dict(usgs=self.__dict__)


class GoogleLandsatContainer(object):
    supported_sensors = {'OLI_TIRS': 'L8', 'LANDSAT_ETM_SLC_OFF': 'L7', 'LANDSAT_ETM': 'L7',
                         'LANDSAT_TM': 'L5', 'TIRS': 'L8', 'OLI': 'L8'}
    base = 'http://storage.googleapis.com/earthengine-public/landsat/%s/%03d/%03d/%s.tar.bz'

    def __init__(self):
        self.link = None

    def to_dict(self):
        return dict(google=self.__dict__)


class PlanetContainer(object):
    def __init__(self):
        self.analytic = None
        self.visual = None

    def to_dict(self):
        return dict(planet=self.__dict__)
