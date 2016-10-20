#-*- coding: utf-8 -*-

""" EOSS catalog system
Reads data provided by USGS directly via http or csv files (from http://landsat.usgs.gov/metadatalist.php) and creates Catalog_Dataset objects
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

from api.eoss_api import Api
from manage.landsat_catalog import USGS_HTTP_SERVICE, USGSCatalog
from model.plain_models import USGSOrderContainer, GoogleLandsatContainer, S3PublicContainer, \
    Catalog_Dataset
import datetime
from utilities.web_utils import remote_file_exists
import dateutil


def landsat_harvester(in_csv):
    datasets = []
    header = False
    with open(in_csv, 'r') as f:
        for counter, line in enumerate(f):
            if header == False:
                header = True
            else:
                content_list = line.split(',')
                if (content_list[52] != 'LANDSAT_4'):
                    dataset = Catalog_Dataset()
                    dataset.entity_id = content_list[0]
                    dataset.sensor = content_list[1]
                    dataset.tile_identifier = '%03d%03d' % (int(content_list[6]), int(content_list[7]))
                    dataset.clouds = float(content_list[19])
                    dataset.daynight = str.lower(content_list[24])
                    if dataset.sensor == "LANDSAT_TM":
                        dataset.acq_time = datetime.strptime(content_list[29][:-6], '%Y:%j:%H:%M:%S')
                    else:
                        dataset.acq_time = datetime.strptime(content_list[29][:-8], '%Y:%j:%H:%M:%S')
                    dataset.level = content_list[53]

                    container = dict()
                    container['quicklook'] = content_list[5]
                    sensor = dataset.sensor
                    if sensor == "OLI_TIRS":
                        sensor = "LANDSAT_8"
                    container['metadata'] = USGS_HTTP_SERVICE % (
                        int(content_list[6]), int(content_list[6]), int(content_list[7]), int(content_list[7]), sensor,

                        dataset.acq_time.strftime("%Y-%m-%d"), dataset.acq_time.strftime("%Y-%m-%d"))

                    usgs = USGSOrderContainer()
                    usgs.link = content_list[54]
                    container.update(usgs.to_dict())

                    google = GoogleLandsatContainer()
                    google_sensors = {'OLI_TIRS': 'L8', 'LANDSAT_ETM_SLC_OFF': 'L7', 'LANDSAT_ETM': 'L7',
                                      'LANDSAT_TM': 'L5', 'TIRS': 'L8', 'OLI': 'L8'}
                    google_link = google.base % (
                        google_sensors[content_list[1]], int(content_list[6]), int(content_list[7]), content_list[0])
                    if remote_file_exists(google_link):
                        google.link = google_link
                        container.update(google.to_dict())

                    dataset.resources = container

                    datasets.append(dataset)
                else:
                    print 'skipping...'

                if (counter % 25000) == 0:
                    print counter

    return datasets


def landsat_harvester_line(lines):
    datasets = []
    header = False

    for counter, line in enumerate(lines):
        content_list = line.split(',')
        if (content_list[52] != 'LANDSAT_4'):
            dataset = Catalog_Dataset()
            dataset.entity_id = content_list[0]
            dataset.sensor = content_list[1]
            dataset.tile_identifier = '%03d%03d' % (int(content_list[6]), int(content_list[7]))
            dataset.clouds = float(content_list[19])
            dataset.daynight = str.lower(content_list[24])
            if dataset.sensor == "LANDSAT_TM":
                dataset.acq_time = datetime.datetime.strptime(content_list[29][:-6], '%Y:%j:%H:%M:%S')
            else:
                dataset.acq_time = datetime.datetime.strptime(content_list[29][:-8], '%Y:%j:%H:%M:%S')
            dataset.level = content_list[53]

            container = dict()
            container['quicklook'] = content_list[5]
            sensor = dataset.sensor
            if sensor == "OLI_TIRS":
                sensor = "LANDSAT_8"
            container['metadata'] = USGS_HTTP_SERVICE % (
                int(content_list[6]), int(content_list[6]), int(content_list[7]), int(content_list[7]), sensor,
                dataset.acq_time.strftime("%Y-%m-%d"), dataset.acq_time.strftime("%Y-%m-%d"))

            usgs = USGSOrderContainer()
            usgs.link = content_list[54]
            container.update(usgs.to_dict())

            google = GoogleLandsatContainer()
            google_link = GoogleLandsatContainer.base % (
                GoogleLandsatContainer.supported_sensors[content_list[1]], int(content_list[6]), int(content_list[7]), content_list[0])
            if remote_file_exists(google_link):
                google.link = google_link
                container.update(google.to_dict())

            dataset.resources = container

            datasets.append(dataset)

        if (counter % 25000) == 0:
            print counter

    return datasets


def import_from_file_ls(in_csv):
    datasets = landsat_harvester(in_csv)

    api = Api()
    skipped = list()
    registered = list()

    for c, ds in enumerate(datasets):
        try:
            out = api.create_dataset(ds)

            if not 'title' in str(out):
                registered.append(c)
            else:
                skipped.append(c)
        except Exception, e:
            print e
        if c % 100 == 0:
            print c
            print 'skipped:', skipped
            print 'registered:', registered
            skipped = list()
            registered = list()


def import_from_pipe_ls(lines):
    datasets = landsat_harvester_line(lines)
    api = Api()
    skipped = list()
    registered = list()

    for c, ds in enumerate(datasets):
        try:
            out = api.create_dataset(ds)
            if not 'already' in str(out):
                registered.append(c)
            else:
                skipped.append(c)
        except Exception, e:
            print e
    print 'registered:', registered
    print 'skipped:', skipped


def import_from_landsat_catalog(sensor,start_date, api_url):
    api = Api(api_url)

    max_cloud_ratio = 1.0
    ag_season_start = dateutil.parser.parse(start_date)
    ag_season_end = ag_season_start + datetime.timedelta(days=1)
    aoi_se = (180, -90)
    aoi_nw = (-180, 90)
    aoi_ne = (aoi_se[0], aoi_nw[1])
    aoi_sw = (aoi_nw[0], aoi_se[1])
    aoi = [aoi_nw, aoi_ne, aoi_se, aoi_sw, aoi_nw]

    cat = USGSCatalog()
    # "LANDSAT_8", "LANDSAT_ETM_SLC_OFF", "LANDSAT_ETM"
    datasets = cat.find(sensor, aoi, ag_season_start, ag_season_end, max_cloud_ratio)

    if datasets != None:
        ds_found = list()
        ds_missing = list()
        for counter, ds in enumerate(datasets):
            catalog_ds = api.get_dataset(ds.entity_id)
            if catalog_ds is None or len(catalog_ds) == 0:
                ds_missing.append(ds)
            elif len(catalog_ds) == 1:
                ds_found.append(catalog_ds)
            else:
                print 'More in catalog found: %s (%d)' % (ds.entity_id, len(catalog_ds))
            if (counter % 25) == 0:
                print counter, len(datasets)
        print 'already registered: ', len(ds_found), len(datasets)
        print 'missing: ', len(ds_missing), len(datasets)

        for counter, ds_obj in enumerate(ds_missing):
            new_ds = api.create_dataset(ds_obj)
            if not new_ds is None:
                print new_ds
            if (counter % 25) == 0:
                print counter, len(ds_missing)
    else:
        print 'No data found in catalog for sentinel from %s to %s' % (
        ag_season_start.strftime("%Y-%m-%d"), ag_season_end.strftime("%Y-%m-%d"))