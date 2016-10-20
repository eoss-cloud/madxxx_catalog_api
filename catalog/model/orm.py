#-*- coding: utf-8 -*-

""" EOSS catalog system
catalog objects ORM model used for the db connection
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

from geoalchemy2 import Geometry
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, Float
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.schema import UniqueConstraint

from model import Context
from utilities import GUID


class Catalog_Dataset(Context().getBase()):
    __tablename__ = "global_catalog"
    __table_args__ = (
        UniqueConstraint('entity_id', 'tile_identifier'),
        {'sqlite_autoincrement': True, 'schema': 'catalogue'}
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_id = Column(String, index=True, nullable=False)
    acq_time = Column(DateTime(timezone=False))
    tile_identifier = Column(String, index=True, nullable=False)
    clouds = Column(Float, nullable=False)
    resources = Column(JSONB)
    level = Column(String, index=True, nullable=False)
    daynight = Column(String, index=True, nullable=False)
    sensor = Column(String, index=True, nullable=False)
    time_registered = Column(DateTime(timezone=False))

    def __repr__(self):
        return '<%s: id:%s (%s) [%s]>' % (self.__class__.__name__, self.entity_id, str(self.acq_time), self.tile_identifier)

    def __eq__(self, other):
        """Override the default Equals behavior"""

        if isinstance(other, self.__class__):
            bools = list()
            for k in ['entity_id', 'acq_time', 'tile_identifier', 'clouds']:
                bools.append(str(self.__dict__[k]).replace('+00:00', '') == str(other.__dict__[k]).replace('+00:00', ''))
            return all(bools)
        return False


class EossProject(Context().getBase()):
    __tablename__ = 'project'
    __table_args__ = (
        UniqueConstraint('id', name='uq_project_identfier'),
        UniqueConstraint('uuid', name='uq_project_uuid'),
        {'sqlite_autoincrement': True, 'schema': 'staging'}
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(GUID, index=True, nullable=False)
    name = Column(String, nullable=False)
    project_start = Column(DateTime(timezone=True))
    project_end = Column(DateTime(timezone=True))
    geom = Column(Geometry('POLYGON', srid=4326), nullable=False)

    def __repr__(self):
        return "<Project(name=%s, start=%s)>" % (
            self.uuid, self.identifier)


class Spatial_Reference_type(Context().getBase()):
    __tablename__ = 'spatialreferencetype'
    __table_args__ = (
        {'sqlite_autoincrement': True, 'schema': 'catalogue'}
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=False)
    shortcut = Column(String, nullable=True)


class Spatial_Reference(Context().getBase()):
    __tablename__ = 'spatialreference'
    __table_args__ = (
        {'sqlite_autoincrement': True, 'schema': 'catalogue'}
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    ref_id = Column(String, nullable=False)
    ref_name = Column(String, nullable=False)
    geom = Column(Geometry('POLYGON', srid=4326), nullable=False)
    referencetype_id = Column(Integer, ForeignKey(Spatial_Reference_type.id))
    referencetype = relationship("Spatial_Reference_type", uselist=False)

    def __repr__(self):
        return '<%s> %s, %d>' % (self.__class__.__name__, self.ref_name, self.referencetype_id)


class SensorAggregation(Context().getBase()):
    __tablename__ = "sensor_aggregation"
    __table_args__ = (
        UniqueConstraint('sensor', 'level', 'aggregation_type'),
        {'sqlite_autoincrement': True, 'schema': 'catalogue'}
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    sensor = Column(String, ForeignKey(Catalog_Dataset.sensor), index=True, nullable=False)
    level = Column(String, ForeignKey(Catalog_Dataset.level), index=True, nullable=False)
    aggregation_type = Column(String, index=True, nullable=False)
    aggregation_name = Column(String, index=True, nullable=False)

