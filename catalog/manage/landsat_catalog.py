# _____________________________________________________________________
# | ................................................................... |
# | ................................................@@@@............... |
# | .@...........................................@@@@@@@@@..@@@@....... |
# | .@@@........................................@@.......@@@@@@@@@@.... |
# | .@.@@@.....................................@.........@.......@@.... |
# | .@...@@@...................................@........@.............. |
# | .@.....@@@.............@@@@@@@@@@@@.......@@.......@............... |
# | .@.......@@@.........@@@.........@@@@.....@........@............... |
# | .@@........@@@.....@@...............@@@...@@.......@............... |
# | .@@...........@@@.@...................@@...@.......@............... |
# | .@@.............@@@....................@@..@@......@@.............. |
# | .@@.............@..@@@..................@@..@@......@@............. |
# | .@@............@......@@@................@@..@@@....@@@............ |
# | .@@...........@..........@@@..............@...@@@@....@@@.......... |
# | .@@...........@.............@@@@..........@@....@@@@...@@@@........ |
# | .@@...........@.................@@@@@......@.......@@@...@@@@...... |
# | .@@@@........@......................@@@@@@@@.........@@.....@@@.... |
# | .@@@@@@......@.............................@..........@@.....@@@... |
# | .@@..@@@@....@.............................@...........@@......@@.. |
# | .@@.....@@@@.@.............................@............@@......@@. |
# | .@@.......@@@@@............................@.............@.......@@ |
# | .@@.........@@@@@..........................@.............@.......@@ |
# | .@............@@@@@.......................@..............@.......@@ |
# | .@............@@..@@@@....................@..............@........@ |
# | .@.............@@...@@@@@................@...............@........@ |
# | .@.............@@@.....@@@@@@...........@@..............@.........@ |
# | .@..............@@@........@@@@@@......@@..............@.........@. |
# | @@@@.............@@@...........@@@@@@@@@...........@@@@.........@.. |
# | ..@@@@@@...........@@@.............@@@@@@@@@@@@@@@@@...........@... |
# | .....@@@@@@@.........@@@@@......@@@@..........................@.... |
# | ..........@@@@@@@......@@@@@@@@@@@..........................@@..... |
# | ...............@@@@@@@@..................................@@@....... |
# | ....................@@@@@@@@@@@......................@@@@@......... |
# | ...........................@@@@@@@@@@@@@@@@@@@@@@@@@@@............. |
# | .....................................@@@@@@@@@@@................... |
# | ................................................................... |
# | ___________________________________________________________________ |
#
# Created by sgebhardt at 15.08.16
# Copyright EOSS GmbH 2016
import cStringIO as StringIO
from urllib import urlencode

import datetime
import geojson
import requests
import xmltodict
from pytz import UTC

from api.eoss_api import Api
from manage import ICatalog
from model.plain_models import USGSOrderContainer, GoogleLandsatContainer, Catalog_Dataset
from utilities.web_utils import remote_file_exists
import logging

logger = logging.getLogger(__name__)


USGS_HTTP_SERVICE = "http://earthexplorer.usgs.gov/EE/InventoryStream/pathrow?" \
                    + "start_path=%d&end_path=%d&start_row=%d&end_row=%d&sensor=%s&" \
                    + "start_date=%s&end_date=%s"

SUPPORTED_SENSORS = ('LANDSAT_8', 'LANDSAT_7', 'LANDSAT_5')


class USGSCatalog(ICatalog):
    sensors = ["LANDSAT_8", "LANDSAT_ETM_SLC_OFF", "LANDSAT_ETM", "LANDSAT_TM"]

    def __init__(self, fast_load=False):
        self.fast_load = fast_load
        self.url = 'https://earthexplorer.usgs.gov/EE/InventoryStream/latlong?'
        # self.url = 'http://landsat.usgs.gov/includes/scripts/get_metadata.php?'

    def find(self, sensor, aoi, date_start, date_stop, cloud_ratio=1.0):
        '''
        :param sensor: string ("LANDSAT_8", "LANDSAT_ETM_SLC_OFF", "LANDSAT_ETM", "LANDSAT_TM")
        :param aoi: geojson polygon
        :param date_start: timestamp
        :param date_stop: timestamp
        :param cloud_ratio: number (0 - 1)
        :return:
        '''

        west = min([x[0] for x in aoi])
        east = max([x[0] for x in aoi])
        south = min([x[1] for x in aoi])
        north = max([x[1] for x in aoi])
        assert south < north, 'south: %f should be smaller than north: %f' % (south, north)
        assert west < east, 'east: %f sould be larger than west: %f' % (east, west)
        assert sensor in SUPPORTED_SENSORS, 'sensor %s should be %s' % (sensor, str(SUPPORTED_SENSORS))
        assert date_start < date_stop, 'Start date should be earlyier than stop date [%s, %s]' % (str(date_start), str(date_stop))
        query = {'north': north, 'south': south, 'east': east, 'west': west,
                 "sensor": sensor, 'start_date': date_start.date(),
                 'end_date': date_stop.date()}

        logger.info('Requesting %s' % self.url + urlencode(query))
        req = requests.get(self.url + urlencode(query), stream=True)
        if req.status_code == requests.codes.ok:
            output = StringIO.StringIO()
            output.write(req.content)

            result = output.getvalue()
            output.close()

            xml = xmltodict.parse(result)

            datasets = set()
            if 'metaData' in xml[u'searchResponse'].keys():
                for ds in xml[u'searchResponse']['metaData']:
                    g1 = geojson.Polygon([[float(ds['upperLeftCornerLongitude']), float(ds['upperLeftCornerLatitude']),
                                           float(ds['lowerLeftCornerLongitude']), float(ds['lowerLeftCornerLatitude']),
                                           float(ds['lowerRightCornerLongitude']), float(ds['lowerRightCornerLatitude']),
                                           float(ds['upperRightCornerLongitude']), float(ds['upperRightCornerLatitude']),
                                           float(ds['upperLeftCornerLongitude']), float(ds['upperLeftCornerLatitude'])]])
                    eoss_ds = Catalog_Dataset()
                    eoss_ds.entity_id = ds['sceneID']
                    eoss_ds.acq_time = ds['acquisitionDate']
                    eoss_ds.sensor = ds['sensor']
                    eoss_ds.tile_identifier = '%s%s' % (ds['path'], ds['row'])
                    eoss_ds.clouds = ds['cloudCoverFull']
                    eoss_ds.level = ds['DATA_TYPE_L1']
                    eoss_ds.daynight = ds['dayOrNight']
                    eoss_ds.resources = dict()

                    container = dict()
                    if ds['browseAvailable'] == 'Y':
                        container['quicklook'] = ds['browseURL']
                    usgs = USGSOrderContainer()
                    usgs.link = ds['cartURL']
                    container.update(usgs.to_dict())

                    google = GoogleLandsatContainer()
                    google.link = GoogleLandsatContainer.base % (GoogleLandsatContainer.supported_sensors[ds['sensor']],
                                                                 int(ds['path']), int(ds['row']), ds['sceneID'])
                    if self.fast_load:
                        container.update(google.to_dict())
                        eoss_ds.resources = container
                    else:
                        if remote_file_exists(google.link):
                            container.update(google.to_dict())
                            eoss_ds.resources = container

                    datasets.add(eoss_ds)

        return datasets

    def register(self):
        raise Exception('Cannot register dataset in repository %s' % self.__class__.__name__)
