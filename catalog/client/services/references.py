#-*- coding: utf-8 -*-

""" EOSS catalog system
 functionality for the references endpoint
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

import ujson
import logging
import time

import falcon
import numpy
from fuzzywuzzy import fuzz
from shapely.geometry import Polygon

from client.services.static_maps import j2_env
from .db_calls import Persistance
from .tools import make_GeoJson, get_base_url, can_zip_response, compress_body

THRES = 80
PRIORITY = {9: 0, 12: 1, 14: 2}  # TODO: Should be part of spatialreferencetype table , 13: 5


class Reference():
    """
    sample: http://localhost:8000/reference/9/73.json (e.g. France)
            http://localhost:8000/reference/9/73.geojson
            http://localhost:8000/reference/9/73.html
            http://localhost:8000/reference/9/all.html?bbox=6,45,12,55

            http://localhost:8000/reference/search/count?entity_name=puebla
    """

    # cors = cors

    def __init__(self, my_router):
        """
        Load all reference objects and reference types in memory for faster lookup

        :param my_router:
        """
        self.router = my_router
        self.logger = logging.getLogger('eoss.' + __name__)
        self.ref_objects = Persistance().get_all_references()
        self.ref_groups = dict()
        for obj in Persistance().get_all_reference_types():
            self.ref_groups[obj.id] = obj

    def on_get(self, req, resp, group_id, reference_id, format):
        """Handles GET requests"""

        start_time = time.time()
        results = dict()
        minx, miny, maxx, maxy = None, None, None, None

        if reference_id == 'all' and 'bbox' in req.params:
            minx, miny, maxx, maxy = [float(x) for x in req.params['bbox']]
            polygon = Polygon([(minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny), (minx, miny)])

            ref_objects = Persistance().get_reference_by_groupid_polygon(group_id, polygon.wkt)
        elif reference_id != 'all':
            ref_objects = Persistance().get_reference_by_groupid_reference_id(group_id, reference_id)
        else:
            description = 'Please specify entity_name OR bbox with request, given %s:%s.' % (reference_id, req.params.get('bbox'))
            raise falcon.HTTPBadRequest('DateFormat', description,
                                        href='http://docs.example.com/auth')

        results['counter'] = ref_objects.count()
        geoms, attrs = list(), list()
        extents = list()
        for ref_objs in ref_objects.all():
            ref_obj, geojson, extent = ref_objs
            extents.append(ujson.loads(extent))
            geoms.append(ujson.loads(geojson))
            attrs.append({"ref_name": ref_obj.ref_name,
                          "reference_id": ref_obj.ref_id,
                          'group_id': self.ref_groups[ref_obj.referencetype_id].id,
                          'group_name': self.ref_groups[ref_obj.referencetype_id].name,
                          'group_description': self.ref_groups[ref_obj.referencetype_id].description
                          })

        results['geojson'] = make_GeoJson(geoms, attrs)
        # return plain geojson object

        results['processing_time'] = time.time() - start_time
        content_type = 'application/json'
        if format == 'json':
            del results['geojson']
            results['attributes']= attrs
        elif format == 'geojson':
            results = results['geojson']
        elif format == 'html':
            if minx != None and miny != None and maxx != None and maxy != None:
                global_extent = [[miny, minx], [maxy, maxx]]
            else:
                mins = list()
                maxs = list()
                for ext in extents:
                    coords = numpy.array(ext[u'coordinates'])
                    mins.append(numpy.min(coords, axis=1))
                    maxs.append(numpy.max(coords, axis=1))
                ext_min, ext_max = list(numpy.min(mins, axis=1)), list(numpy.max(maxs, axis=1))
                global_extent = [
                    [ext_min[0][1], ext_min[0][0]],
                    [ext_max[0][1], ext_max[0][0]]]

            content_type = 'text/html'
            results = j2_env.get_template('leaflet_map.html').render(title='Reference object: %s' % group_id, center='[%f, %f]' % (21.5, -102),
                                                                zoomlevel=5, geojson=ujson.dumps(results['geojson']),
                                                                     label_attribute='ref_name',
                                                                extent=ujson.dumps(global_extent))
        else:
            description = 'Unknown format given %s.' % (format)
            raise falcon.HTTPBadRequest('Reference', description,
                                        href='http://docs.example.com/auth')

        if can_zip_response(req.headers):
            resp.set_header('Content-Type', content_type)
            resp.set_header('Content-Encoding', 'gzip')
            if content_type == 'application/json':
                resp.body = compress_body(ujson.dumps(results))
            else:
                resp.body = compress_body(results)
        else:
            resp.set_header('Content-Type', content_type)
            if content_type == 'application/json':
                resp.body = ujson.dumps(results)
            else:
                resp.body = results
        resp.status = falcon.HTTP_200


class ReferenceSearcher():
    """
    http://localhost:8000/reference/search/count?entity_name=Mexico
    """

    # cors = cors

    def __init__(self, my_router):
        self.router = my_router
        self.logger = logging.getLogger('eoss.' + __name__)

        self.ref_objects = Persistance().get_selected_references(PRIORITY.keys())
        self.ref_groups = dict()
        for obj in Persistance().get_all_reference_types():
            self.ref_groups[obj.id] = obj

    def on_get(self, req, resp):
        """Handles GET requests"""

        BASE_URL = get_base_url(req.url)

        start_time = time.time()
        results = dict()
        results['entities'] = list()
        results['fuzzyness'] = 5
        results['total_count'] = len(self.ref_objects)
        results['Levenshtein_distance_threshold'] = THRES
        results['total_groups'] = len(self.ref_groups)
        results['searched_groups'] = len(PRIORITY.keys())

        if 'entity_name' not in req.params.keys():
            description = 'entity_name not specified in query '
            raise falcon.HTTPBadRequest('KeyError', description,
                                        href='http://docs.example.com/auth')

        entity_name = req.params['entity_name']
        entity_name_length = len(entity_name)
        counter = 0
        similarities = list()

        for ref_obj in self.ref_objects:
            if ref_obj[0][:entity_name_length].lower() == entity_name.lower():
                similarities.append((100 - PRIORITY[ref_obj.referencetype_id], ref_obj.ref_name, ref_obj.referencetype_id, ref_obj.ref_id))
            elif entity_name.lower() in ref_obj[0].lower():
                similarities.append((100 - PRIORITY[ref_obj.referencetype_id], ref_obj.ref_name, ref_obj.referencetype_id, ref_obj.ref_id))

        if len(similarities) < results['fuzzyness']:
            word_size = -1  # len(entity_name)
            for ref_obj in self.ref_objects:
                if ref_obj.ref_name not in [x[1] for x in similarities]:
                    sim = fuzz.partial_ratio(entity_name, ref_obj.ref_name[:word_size])
                    if sim > THRES:
                        similarities.append((sim - 1, ref_obj.ref_name, ref_obj.referencetype_id, ref_obj.ref_id))
                        counter += 1

        sorted_similarities = sorted(set(similarities), key=lambda tup: (-tup[0], tup[1]), reverse=True)
        sorted_similarities.reverse()

        for item in sorted_similarities:
            results['entities'].append({'entity_id': item[1], 'distance': item[0], 'reference_id': item[3],
                                        'resource_json': BASE_URL + self.router.reverse('reference_entity',
                                                                                        group_id=self.ref_groups[item[2]].id,
                                                                                        reference_id=item[3],
                                                                                        format='json'),
                                        'resource_geojson': BASE_URL + self.router.reverse('reference_entity',
                                                                                           group_id=self.ref_groups[item[2]].id,
                                                                                           reference_id=item[3],
                                                                                           format='geojson'),
                                        'resource_html': BASE_URL + self.router.reverse('reference_entity',
                                                                                        group_id=self.ref_groups[item[2]].id,
                                                                                        reference_id=item[3],
                                                                                        format='html'),
                                        'entity_group': {
                                            'group_id': self.ref_groups[item[2]].id,
                                            'group_name': self.ref_groups[item[2]].name,
                                            'group_description': self.ref_groups[item[2]].description,
                                            'group_shortcut': self.ref_groups[item[2]].shortcut
                                        }})

        results['counter'] = counter
        results['processing_time'] = time.time() - start_time
        resp.status = falcon.HTTP_200

        if can_zip_response(req.headers):
            resp.set_header('Content-Type', 'application/json')
            resp.set_header('Content-Encoding', 'gzip')
            resp.body = compress_body(ujson.dumps(results))
        else:
            resp.set_header('Content-Type', 'application/json')
            resp.body = ujson.dumps(results)
