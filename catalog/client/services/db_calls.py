#-*- coding: utf-8 -*-

""" EOSS catalog system
 database connection for the service endpoints
"""
from sqlalchemy.sql.sqltypes import Date

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

import logging

import geoalchemy2
import ujson
from geoalchemy2.elements import WKTElement
from sqlalchemy import func, String
from sqlalchemy import or_
from sqlalchemy.orm import aliased
from sqlalchemy import text

from model import Context
from model.orm import Catalog_Dataset, SensorAggregation, Spatial_Reference, Spatial_Reference_type
#from .simple_cache import region
from client.services.simple_cache import region


class Persistance:
    def __init__(self):
        self.logger = logging.getLogger('eoss.' + __name__)
        self.session = Context().getSession()
        self.engine = Context().get_engine()

    #@region.cache_on_arguments()
    def get_sensors(self, group):
        sensor_agg = func.array_agg(SensorAggregation.sensor, type_=String).label('sensor_agg')
        level_agg = func.array_agg(SensorAggregation.level, type_=String).label('level_agg')
        return self.session.query(SensorAggregation.aggregation_name, sensor_agg, level_agg).filter(
            SensorAggregation.aggregation_type == group).distinct().group_by(SensorAggregation.aggregation_name).order_by(
            SensorAggregation.aggregation_name)

    def delete_dataset(self, entity_id):
        self.session.query(Catalog_Dataset).filter(Catalog_Dataset.entity_id == entity_id).delete(synchronize_session=False)
        self.session.commit()

    #@region.cache_on_arguments()
    def get_dataset(self, entity_id):
        ds = self.session.query(Catalog_Dataset).filter(Catalog_Dataset.entity_id == entity_id).all()
        return ds

    def get_dataset_by_sensor_and_date(self,sensor,acq_date):
        ds = self.session.query(Catalog_Dataset).filter(Catalog_Dataset.sensor == sensor).filter(func.DATE(Catalog_Dataset.acq_time)==func.DATE(acq_date)).all()
        return ds

    def get_observation_coverage(self, reference_type_id, last_days=2):
        struct = dict()
        struct['geojson'] = list()
        struct['attr'] = list()
        sql = text('select r.ref_name, st_asgeojson(st_centroid(r.geom)), max(ds.acq_time::date), count(ds.*) from catalogue.spatialreference r, CATALOGue.global_catalog ds where r.referencetype_id = %d and ds.tile_identifier = r.ref_name and ds.acq_time::date > now()::date-%d group by r.ref_name, st_asgeojson(st_centroid(r.geom)) order by r.ref_name'%(reference_type_id, last_days))

        sql = text('select r.ref_name, st_asgeojson(r.geom), max(ds.acq_time::date), count(ds.*) from catalogue.spatialreference r, CATALOGue.global_catalog ds where r.referencetype_id = %d and ds.tile_identifier = r.ref_name and ds.acq_time::date > now()::date-%d group by r.ref_name, st_asgeojson(r.geom) order by r.ref_name'%(reference_type_id, last_days))
        result = self.engine.execute(sql)
        for r in result:
            struct['geojson'].append(ujson.loads(r[1]))
            struct['attr'].append({
                'tile_id':r[0],
                'count':r[3],
                'last_observation': r[2].isoformat(),
            })
        return struct

    def add_dataset(self, obj):
        session = Context().getSession()
        ds_exists = session.query(Catalog_Dataset).filter(
            Catalog_Dataset.entity_id == obj.entity_id).filter(
            Catalog_Dataset.tile_identifier == obj.tile_identifier).filter(
            Catalog_Dataset.acq_time == obj.acq_time)
        if not ds_exists.count():
            try:
                c = Catalog_Dataset(**dict(obj))
                session.add(c)
                session.commit()
            except:
                session.rollback()
                raise
            finally:
                session.close()
                Context().closeSession()
        return True

    #@region.cache_on_arguments()
    def get_all_sensor_aggregations(self):
        return self.session.query(SensorAggregation).all()

    @region.cache_on_arguments()
    def get_all_tilegrid(self):
        return self.session.query(Spatial_Reference.ref_name,geoalchemy2.functions.ST_AsGeoJSON(Spatial_Reference.geom)).all()

    def get_reference_by_sensorgrid(self, ref_id, ref_type_id, sensor_grid):
        sat_grid = aliased(Spatial_Reference)
        ref_obj = aliased(Spatial_Reference)
        return self.session.query(sat_grid.ref_name).filter(
            sat_grid.geom.ST_Intersects(ref_obj.geom)
        ).filter(ref_obj.referencetype_id == ref_type_id).filter(ref_obj.ref_id == ref_id).filter(
            sat_grid.referencetype_id == sensor_grid)

    def get_reference(self, ref_id, ref_type_id):
        return self.session.query(Spatial_Reference).filter(Spatial_Reference.referencetype_id == ref_type_id).filter(
            Spatial_Reference.ref_id == ref_id)

    def get_referencebyaoi(self, wkt, sensor_grid):
        sat_grid = aliased(Spatial_Reference)
        wkt = WKTElement(wkt, srid=4326)
        return self.session.query(sat_grid.ref_name).filter(sat_grid.referencetype_id == sensor_grid).filter(
            sat_grid.geom.ST_Intersects(wkt))

    def find_dataset(self, dates_filter, sensors_filter, grid_list, joint_gridset, clouds):
        if len(sensors_filter) > 0 and len(dates_filter) > 0 and len(grid_list[11]) > 0:
            query = self.session.query(Catalog_Dataset).filter(Catalog_Dataset.daynight == 'day').filter(
                Catalog_Dataset.clouds <= clouds).filter(
                or_(*sensors_filter)).filter(or_(*dates_filter)).filter(
                Catalog_Dataset.tile_identifier.in_(joint_gridset))
        elif len(sensors_filter) == 0 and len(dates_filter) > 0 and len(grid_list[11]) > 0:
            query = self.session.query(Catalog_Dataset).filter(Catalog_Dataset.daynight == 'day').filter(
                Catalog_Dataset.clouds <= clouds).filter(
                or_(*dates_filter)).filter(Catalog_Dataset.tile_identifier.in_(joint_gridset))
        elif len(sensors_filter) > 0 and len(dates_filter) > 0 and len(grid_list[11]) == 0:
            query = self.session.query(Catalog_Dataset).filter(Catalog_Dataset.daynight == 'day').filter(
                Catalog_Dataset.clouds <= clouds).filter(
                or_(*sensors_filter)).filter(or_(*dates_filter))
        elif len(sensors_filter) == 0 and len(dates_filter) > 0 and len(grid_list[11]) == 0:
            query = self.session.query(Catalog_Dataset).filter(Catalog_Dataset.daynight == 'day').filter(
                Catalog_Dataset.clouds <= clouds).filter(
                or_(*dates_filter))

        return query

    @region.cache_on_arguments()
    def get_tile_geom(self, tiles_list):
        return self.session.query(Spatial_Reference.ref_name, geoalchemy2.functions.ST_AsGeoJSON(Spatial_Reference.geom)).filter(
            Spatial_Reference.ref_name.in_(tiles_list))

    @region.cache_on_arguments()
    def get_all_reference_types(self):
        return self.session.query(Spatial_Reference_type).all()

    @region.cache_on_arguments()
    def get_all_references(self):
        return self.session.query(Spatial_Reference.ref_name, Spatial_Reference.referencetype_id, Spatial_Reference.ref_id).distinct().all()

    def get_reference_by_groupid_polygon(self, group_id, polygon_wkt):
        return self.session.query(Spatial_Reference, geoalchemy2.functions.ST_AsGeoJSON(Spatial_Reference.geom),
                                  geoalchemy2.functions.ST_AsGeoJSON(geoalchemy2.functions.ST_Envelope(Spatial_Reference.geom))).filter(
            Spatial_Reference.referencetype_id == group_id).filter(Spatial_Reference.geom.ST_Intersects('SRID=4326;' + polygon_wkt))

    def get_reference_by_groupid_reference_id(self, group_id, reference_id):
        return self.session.query(Spatial_Reference, geoalchemy2.functions.ST_AsGeoJSON(Spatial_Reference.geom),
                                  geoalchemy2.functions.ST_AsGeoJSON(geoalchemy2.functions.ST_Envelope(Spatial_Reference.geom))
                                  ).filter(
            Spatial_Reference.referencetype_id == group_id).filter(
            Spatial_Reference.ref_id == reference_id)

    def get_reference_by_groupid_reference_name(self, group_id, reference_name):
        return self.session.query(Spatial_Reference, geoalchemy2.functions.ST_AsGeoJSON(Spatial_Reference.geom),
                                  geoalchemy2.functions.ST_AsGeoJSON(geoalchemy2.functions.ST_Envelope(Spatial_Reference.geom))
                                  ).filter(
            Spatial_Reference.referencetype_id == group_id).filter(
            Spatial_Reference.ref_name == reference_name)

    @region.cache_on_arguments()
    def get_selected_references(self, REGISTERED_REF_TYPES):
        return self.session.query(Spatial_Reference.ref_name, Spatial_Reference.referencetype_id, Spatial_Reference.ref_id).filter(
            Spatial_Reference.referencetype_id.in_(REGISTERED_REF_TYPES)).distinct().all()

    def get_dataset_tiles_date_clouds(self, sensors_filter, dates_filter, tiles_list):
        if len(sensors_filter) > 0 and len(dates_filter) > 0:
            query = self.session.query(Catalog_Dataset.tile_identifier, Catalog_Dataset.acq_time, Catalog_Dataset.clouds).filter(
                or_(*sensors_filter)).filter(or_(*dates_filter)).filter(Catalog_Dataset.tile_identifier.in_(tiles_list)).order_by(
                Catalog_Dataset.tile_identifier, Catalog_Dataset.acq_time)
        elif len(sensors_filter) == 0 and len(dates_filter) > 0:
            query = self.session.query(Catalog_Dataset.tile_identifier, Catalog_Dataset.acq_time, Catalog_Dataset.clouds).filter(
                or_(*dates_filter)).filter(Catalog_Dataset.tile_identifier.in_(tiles_list)).order_by(Catalog_Dataset.tile_identifier,
                                                                                                     Catalog_Dataset.acq_time)
        return query



if __name__ == '__main__':
    group_id, tile_identifier = 10, '32UPA'
    result = Persistance().get_dataset_by_sensor_and_date('LANDSAT_ETM', '2000-01-01')
    print result
    # print a
