#-*- coding: utf-8 -*-

""" EOSS catalog system
 functionality for the catalog endpoint
"""
from utilities.web_utils import remote_file_exists

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

import datetime
import ujson
import time

import dateparser
import falcon

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

import csv
from xlsxwriter import Workbook
from dateutil.parser import parse
import numpy
from sqlalchemy import and_
import logging
from collections import defaultdict

from model.orm import Catalog_Dataset, Spatial_Reference
from api import General_Structure

from .db_calls import Persistance
from . import getKeysFromDict
from .tools import get_base_url, can_zip_response, compress_body, serialize, make_GeoJson


class Catalog(object):
    """
       EOSS catalog class from web API
       """

    def __init__(self):
        self.logger = logging.getLogger('eoss.' + __name__)
        self.aggregations = defaultdict(list)
        for agg in Persistance().get_all_sensor_aggregations():
            self.aggregations[agg.aggregation_name.lower()].append(agg)

    def _query_(self, areas, dates, sensors, clouds):
        GRID_SYSTEMS = {'Sentinel - 2A': 10,
                        'LANDSAT_ETM': 11,
                        'LANDSAT_ETM_SLC_OFF': 11,
                        'OLI_TIRS': 11,
                        'TIRS': 11}

        sensors_filter = list()
        grid_list = defaultdict(set)

        for sensor_grid in set(GRID_SYSTEMS.values()):
            if 'ref_group' in areas[0].keys():
                ref_type_id, ref_id = areas[0]['ref_group'], areas[0]['ref_id']
                spatial_query = Persistance().get_reference_by_sensorgrid(ref_id, ref_type_id, sensor_grid)
            elif 'aoi' in areas[0].keys():
                aoi = areas[0]['aoi']
                spatial_query = Persistance().get_referencebyaoi(aoi, sensor_grid)
            for grid in spatial_query.all():
                grid_list[sensor_grid].add(grid)

        if len(grid_list) == 0:
            description = 'Please specify valid reference object for data. (type:%s, id:%s)' \
                          % (ref_type_id, ref_id)
            raise falcon.HTTPBadRequest('SensorGrid', description,
                                        href='http://docs.example.com/auth')

        joint_gridset = grid_list[10] | grid_list[11]  # TODO: better grid system handling from extra table?

        for item in sensors:
            sensor, level = item['sensor_name'], item['level']

            if len(sensor) > 0 and len(level) > 0:
                sensors_filter.append(and_(Catalog_Dataset.level == level, Catalog_Dataset.sensor == sensor))
            elif len(sensor) == 0 and len(level) > 0:
                sensors_filter.append(Catalog_Dataset.level == level)
            elif len(sensor) > 0 and len(level) == 0:
                sensors_filter.append(Catalog_Dataset.sensor == sensor)

        dates_filter = list()
        for item in dates:
            # ExtJS POST requests has provides unicode body
            if type(item["start_date"]) is unicode:
                item["start_date"] = parse(item["start_date"])
            if type(item["end_date"]) is unicode:
                item["end_date"] = parse(item["end_date"])

            dates_filter.append(
                and_(Catalog_Dataset.acq_time >= item["start_date"].isoformat(), Catalog_Dataset.acq_time <= item["end_date"].isoformat()))

        query = Persistance().find_dataset(dates_filter, sensors_filter, grid_list, joint_gridset, clouds)
        return query

    def _get_datasets(self, query):
        query_result = list()
        for ds in query:
            values = dict()
            types = dict()
            for k, v in ds.__dict__.iteritems():
                if '_' != k[0]:
                    values[k] = v
                    types[k] = type(v)
            x = General_Structure(values, types)
            x.__class__.__name__ = 'Catalog_Dataset'

            query_result.append(serialize(x, as_json=False)['data'])

        return query_result

    # TODO: tiles list as input - only first will be returned or exception thrown !
    def _query_tile_geom(self, tiles):
        tile_objs = Persistance().get_tile_geom(tiles)
        return tile_objs.all()

    def _export_query(self, found_dataset):
        row_keys = ['tile_identifier', 'entity_id', 'acq_time', 'clouds']
        resources = [('resources', 'metadata'), ('resources', 'quicklook')]

        row = list()
        rows = list()
        for k in row_keys:
            row.append(k)
        for k in resources:
            row.append(' '.join(k))
        row.append('data')
        rows.append(row)

        for ds in found_dataset:
            row = list()
            for k in row_keys:
                row.append(ds.get(k))
            for k in resources:
                row.append(getKeysFromDict(ds, k))
            if ds.get('sensor') in ['LANDSAT_TM', 'LANDSAT_ETM', 'LANDSAT_ETM_SLC_OFF']:
                if 'google' in ds.get('resources').keys():
                    row.append(getKeysFromDict(ds, ('resources', 'google', 'link')))
                elif 'usgs' in ds.get('resources').keys():
                    row.append(getKeysFromDict(ds, ('resources', 'usgs', 'link')))
                else:
                    row.append('?')
            elif ds.get('sensor') in ['OLI_TIRS', 'OLI', 'TIRS']:
                if 's3public' in ds.get('resources').keys():
                    row.append(getKeysFromDict(ds, ('resources', 's3public', 'zip')))
                elif 'google' in ds.get('resources').keys():
                    row.append(getKeysFromDict(ds, ('resources', 'google', 'link')))
            elif ds.get('sensor') in ['Sentinel-2A']:
                if 's3public' in ds.get('resources').keys():
                    if getKeysFromDict(ds, ('resources', 's3public')) != None:
                        row.append(getKeysFromDict(ds, ('resources', 's3public', 'zip')))
                    else:
                        row.append('?')
                else:
                    row.append('?')

            rows.append(row)
        return rows


class CatalogApi(Catalog):
    def __init__(self, my_router):
        Catalog.__init__(self)
        self.router = my_router


    def on_get(self, req, resp, format, check_resources=True):
        """Handles GET requests
        http://localhost:8000/catalog/search/result.json?from_date=2016-05-01&to_date=2016-06-02&sensor=sentinel2&ref_group=9&ref_id=73&clouds=50
        """

        BASE_URL = get_base_url(req.url)
        start_time = time.time()
        query_filter = req.params
        results = dict()
        results['action'] = 'catalog search'
        results['action-time'] = str(datetime.datetime.now())
        results.update({'query': query_filter})
        dates = list()
        sensor_list = list()

        try:
            for date_string in ['from_date', 'to_date']:
                date = dateparser.parse(req.params[date_string])
                if date is None:
                    description = 'Please format date propery, used %s:%s.' % (date_string, date)
                    raise falcon.HTTPBadRequest('DateFormat', description,
                                                href='http://docs.example.com/auth')
                else:
                    dates.append(date)

            if dates[0] == dates[1]:
                description = 'Given dates didnt cover date range. Please correct date span. (%s-%s)' \
                              % (req.params['from_date'], req.params['to_date'])
                raise falcon.HTTPBadRequest('DateFormat', description,
                                            href='http://docs.example.com/auth')
            elif dates[0] > dates[1]:
                description = 'Given end date is before start date. Please reverse dates. (%s-%s)' \
                              % (req.params['from_date'], req.params['to_date'])
                raise falcon.HTTPBadRequest('DateFormat', description,
                                            href='http://docs.example.com/auth')
            if not req.params['sensor'].lower() in self.aggregations.keys():
                description = 'Sensor label is unknown in aggregation table, use %s' % str(map(str, self.aggregations.keys()))
                raise falcon.HTTPBadRequest('DateFormat', description,
                                            href='http://docs.example.com/auth')

            for agg in self.aggregations[req.params['sensor'].lower()]:
                sensor_list.append({"sensor_name": agg.sensor, "level": agg.level})

            ref_group, ref_id, clouds = int(req.params['ref_group']), int(req.params['ref_id']), int(req.params['clouds'])


        except KeyError, e:
            description = 'Search key: %s missing in query.' % e
            raise falcon.HTTPBadRequest('KeyError', description,
                                        href='http://docs.example.com/auth')
        except ValueError, e:
            description = 'Given parameters contain bad values: %s'% str(e)
            raise falcon.HTTPBadRequest('KeyError', description,
                                        href='http://docs.example.com/auth')

        query = self._query_([{"ref_group": ref_group, "ref_id": ref_id}],
                             [{"start_date": dates[0], "end_date": dates[1]}],
                             sensor_list, clouds)
        found_dataset = self._get_datasets(query)
        if check_resources:
            for ds in found_dataset:
                if 's3public' in ds['resources'].keys():
                    if 'zip' in ds['resources']['s3public'].keys():
                        if not remote_file_exists( ds['resources']['s3public']['zip']):
                            print '%s missing' % ds['resources']['s3public']['zip']

        if format.lower() == 'json':
            if 'search/count' in req.url:
                results['count'] = query.count()
            else:
                results['count'] = query.count()
                results['found_dataset'] = found_dataset
                results['found_tiles'] = sorted(list(set([x['tile_identifier'] for x in found_dataset])))
                results['found_resources'] = [BASE_URL + self.router.reverse('dataset_entity', entity_id=x['entity_id'])
                                              for x in results['found_dataset']]
            results['processing_time'] = time.time() - start_time
        elif format.lower() == 'geojson':
            tilegrids = defaultdict(lambda: defaultdict(list))
            geoms, attrs = list(), list()
            for x in found_dataset:
                tilegrids[x['tile_identifier']]['acq_time'].append(x['acq_time'])
                # tilegrids[x['tile_identifier']]['acq_time_js'].append(
                #     int(time.mktime(dateparser.parse(x['acq_time']).timetuple())) * 1000)
                tilegrids[x['tile_identifier']]['tile_identifier'].append(x['tile_identifier'])
                tilegrids[x['tile_identifier']]['clouds'].append(x['clouds'])

            for tile_id in tilegrids.keys():
                tilegrids[tile_id]['count'] = len(tilegrids[tile_id]['clouds'])
                tilegrids[tile_id]['tile_identifier'] = tilegrids[tile_id]['tile_identifier'][0]

            tiles_dict = dict()
            if len(tilegrids.keys()) > 0:
                for ref_name, geom in self._query_tile_geom(tilegrids.keys()):
                    tiles_dict[ref_name] = geom
                for tile_id in tilegrids.keys():
                    geoms.append(ujson.loads(tiles_dict[tile_id]))
                    attrs.append(tilegrids[tile_id])
            results = make_GeoJson(geoms, attrs)
        elif format.lower() == 'csv':
            rows = self._export_query(found_dataset)
            si = StringIO.StringIO()
            cw = csv.writer(si, delimiter='\t')
            for row in rows:
                cw.writerow(row)
            results = si.getvalue().strip('\r\n')
        elif format.lower() == 'xlsx':
            rows = self._export_query(found_dataset)
            strIO = StringIO.StringIO()
            workbook = Workbook(strIO, {'in_memory': True, 'constant_memory': True})
            bold = workbook.add_format({'bold': True})
            big_bold = workbook.add_format({'bold': True, 'size': 20})
            italic = workbook.add_format({'italic': True})

            worksheet = workbook.add_worksheet(name='EOSS analysis')

            worksheet.write(0, 0, 'EOSS data analysis', big_bold)

            ref_obj = Persistance().get_reference(query_filter.get('ref_id'), query_filter.get('ref_group')).one()

            query_filter['reference_name'] = ref_obj.ref_name
            query_filter['reference_type'] = ref_obj.referencetype.name
            # {'clouds': '60', 'ref_id': '5502', 'from_date': '09/07/2016', 'to_date': '10/07/2016', 'ref_group': '12', 'sensor': 'Sentinel2'}
            r = 3
            worksheet.write(r - 1, 0, 'query filter:', big_bold)
            for c, k in enumerate(['sensor', 'from_date', 'to_date', 'clouds', 'reference_name', 'reference_type']):
                worksheet.write(r + c, 0, k, bold)
                worksheet.write(r + c, 1, query_filter[k])

            r = 13
            worksheet.write(r - 2, 0, 'query set:', big_bold)
            for c, k in enumerate(rows[0]):
                worksheet.write(r - 1, c, k, bold)

            for values in rows[1:]:
                for c, v in enumerate(values):
                    worksheet.write(r, c, v)
                r += 1

            workbook.close()
            strIO.seek(0)
            results = strIO.read()

        elif format.lower() == 'hist':
            found_tiles = sorted(list(set([x['tile_identifier'] for x in found_dataset])))
            result_list = []

            first = dict()
            first['tile_identifier'] = 'percentagelabel'
            first['span'] = 100
            result_list.append(first)

            data = numpy.zeros((len(found_dataset)))
            tileslist = []

            i = 0
            for x in found_dataset:
                tileslist.append(x['tile_identifier'])
                data[i] = float(x['clouds'])
                i = i + 1

            for t in found_tiles:
                ix = numpy.array(tileslist) == t
                subset_clouds = data[ix]

                num_scenes = sum(ix)
                hist_abs = numpy.histogram(subset_clouds, bins=[-1] + range(0, 120, 20))
                hist_rel = hist_abs[0] * 1.0 / num_scenes

                hist_struct = dict()
                hist_struct['tile_identifier'] = t
                hist_struct['span'] = 100
                hist_struct['scenes_perc_-1'] = hist_rel[0]
                hist_struct['scenes_perc_20'] = hist_rel[1]
                hist_struct['scenes_perc_40'] = hist_rel[2]
                hist_struct['scenes_perc_60'] = hist_rel[3]
                hist_struct['scenes_perc_80'] = hist_rel[4]
                hist_struct['scenes_perc_100'] = hist_rel[5]
                hist_struct['scenes_abs_-1'] = hist_abs[0][0]
                hist_struct['scenes_abs_20'] = hist_abs[0][1]
                hist_struct['scenes_abs_40'] = hist_abs[0][2]
                hist_struct['scenes_abs_60'] = hist_abs[0][3]
                hist_struct['scenes_abs_80'] = hist_abs[0][4]
                hist_struct['scenes_abs_100'] = hist_abs[0][5]
                result_list.append(hist_struct)

            results['found_tiles'] = result_list

        resp.status = falcon.HTTP_200

        if can_zip_response(req.headers):
            if format.lower() in ['hist', 'json', 'geojson']:
                resp.set_header('Content-Type', 'application/json')
                resp.set_header('Content-Encoding', 'gzip')
                resp.body = compress_body(ujson.dumps(results))
            elif format.lower() == 'csv':
                resp.set_header('Content-Type', 'text/csv')
                resp.set_header('Content-disposition', 'attachment;filename=%s;' % self.create_output_name('csv'))
                resp.set_header('Content-Encoding', 'gzip')
                resp.body = compress_body(results)
            elif format.lower() == 'xlsx':
                resp.set_header('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                resp.set_header('Content-disposition', 'attachment;filename=%s;' % self.create_output_name('xlsx'))
                resp.set_header('Content-Encoding', 'gzip')
                resp.body = compress_body(results)
        else:
            if format.lower() in ['hist', 'json', 'geojson']:
                resp.set_header('Content-Type', 'application/json')
                resp.body = ujson.dumps(results)
            elif format.lower() == 'csv':
                resp.set_header('Content-Type', 'text/csv')
                resp.set_header('Content-disposition', 'attachment;filename=%s;' % self.create_output_name('csv'))
                resp.body = results
            elif format.lower() == 'xlsx':
                resp.set_header('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                resp.set_header('Content-disposition', 'attachment;filename=%s;' % self.create_output_name('xlsx'))
                resp.body = results

    def create_output_name(self, extension):
        return 'EOSS_analysis_%s.%s' % (datetime.datetime.now().isoformat(), extension)

    def on_post(self, req, resp, format):
        """Handles POST requests
        {
	"daterange": [{
		"start_date": "05/31/2000",
		"end_date": "07/02/2003"
	}],
	"clouds": 1,
	"sensors": [
    {"sensor_name": "LANDSAT_ETM", "level": "" }],
	"areas": [{
		"ref_group": 12,
		"ref_id": 6208
	}]
}

{"clouds":20,"daterange":[{"start_date":"09/02/2015","end_date":"09/14/2016"}],
  "sensors":[{"name":"landsat"}],
    "areas":[{"ref_id":362,"ref_group":"9"}]}
        """

        # TODO: loop over areas
        sensor_list = list()
        results = dict()
        start_time = time.time()
        output = StringIO.StringIO()
        while True:
            chunk = req.stream.read(4096)
            if not chunk:
                break
            output.write(chunk)

        body = output.getvalue()
        output.close()
        try:
            struct = ujson.loads(body.decode('utf-8'))

        except ValueError, e:
            # try decode  x-www-form-urlencoded
            query_str = falcon.util.uri.decode(body.decode('utf-8'))
            query_str = query_str[query_str.find('{'):query_str.rfind('}') + 1]
            try:
                struct = ujson.loads(query_str)
            except ValueError, e:
                description = 'Give request is no valid JSON nor urlencoded psot body.'
                raise falcon.HTTPUnsupportedMediaType(description,
                                                      href='http://docs.example.com/auth')

        try:
            for s in struct['sensors']:
                if 'sensor_name' in s.keys() and 'level' in s.keys():
                    sensor_list.append(s)
                elif 'name' in s.keys():
                    if not s['name'].lower() in self.aggregations.keys():
                        description = 'Sensor label is unknown in aggregation table'
                        raise falcon.HTTPBadRequest('Catalog', description,
                                                    href='http://docs.example.com/auth')
                    for agg in self.aggregations[s['name'].lower()]:
                        sensor_list.append({"sensor_name": agg.sensor, "level": agg.level})
                else:
                    description = 'Sensor is not specified in query'
                    raise falcon.HTTPBadRequest('Catalog', description,
                                                href='http://docs.example.com/auth')

            query = self._query_(struct['areas'], struct['daterange'], sensor_list, struct['clouds'])

        except KeyError, e:
            description = 'Search key: %s missing in query.' % e
            raise falcon.HTTPBadRequest('KeyError', description,
                                        href='http://docs.example.com/auth')
        results['count'] = query.count()

        found_dataset = self._get_datasets(query)
        results['found_dataset'] = found_dataset
        results['found_tiles'] = sorted(list(set([x['tile_identifier'] for x in found_dataset])))
        # results.update({'query': struct})

        resp.body = ujson.dumps(results)

        resp.status = falcon.HTTP_200
        results['processing_time'] = time.time() - start_time
        if can_zip_response(req.headers):
            resp.set_header('Content-Type', 'application/json')
            resp.set_header('Content-Encoding', 'gzip')
            resp.body = compress_body(ujson.dumps(results))
        else:
            resp.set_header('Content-Type', 'application/json')
            resp.body = ujson.dumps(results)
