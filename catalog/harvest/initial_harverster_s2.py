#-*- coding: utf-8 -*-

""" EOSS catalog system
Reads sentinel2 data which is stored in AWS buckets extracted with 'aws s3 ls sentinel-s2-l1c/products/ --recursive --region=eu-central-1 | grep productInfo.json'
[
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
import dateutil.parser
import xmltodict
import datetime
from api.eoss_api import Api
from harvest import count_lines
from manage.sentinelcatalog import SENTINEL_S3_BUCKET, SENTINEL_S3_HTTP_ZIP_BASEURL, \
    SENTINEL_S3_HTTP_BASEURL, SentinelCatalog
from model.plain_models import SentinelS3Container, Catalog_Dataset
from utilities.web_utils import remote_file_exists, public_key_exists, public_get_filestream


def sentinel_harvester(in_csv, N, M=1000):
    datasets = []

    with open(in_csv, 'r') as f:
        for counter, line in enumerate(f):
            content_list = line.split(' ')
            tileinfokey = content_list[-1]
            tileinfokey = tileinfokey.rstrip("\n")
            quicklookkey = tileinfokey.replace('tileInfo.json', 'preview.jpg')
            if counter < N + M and counter >= N:
                if public_key_exists(SENTINEL_S3_BUCKET, tileinfokey) and public_key_exists(SENTINEL_S3_BUCKET,
                                                                                            quicklookkey):
                    tilenfodict = ujson.loads(public_get_filestream(SENTINEL_S3_BUCKET, tileinfokey))
                    productkey = tilenfodict['productPath']

                    s3 = SentinelS3Container()
                    s3.bucket = SENTINEL_S3_BUCKET
                    s3.tile = tilenfodict['path'] + '/'
                    s3.quicklook = quicklookkey

                    dataset = Catalog_Dataset()
                    dataset.entity_id = tilenfodict['productName']
                    dataset.tile_identifier = '%02d%s%s' % (
                    tilenfodict['utmZone'], tilenfodict['latitudeBand'], tilenfodict['gridSquare'])
                    dataset.clouds = tilenfodict['cloudyPixelPercentage']
                    dataset.acq_time = dateutil.parser.parse(tilenfodict['timestamp'])

                    if public_key_exists(SENTINEL_S3_BUCKET, productkey + '/metadata.xml'):
                        s3.product = productkey + '/'
                        metadatakey = productkey + '/metadata.xml'
                        metadatadict = xmltodict.parse(
                            public_get_filestream(SENTINEL_S3_BUCKET, metadatakey))

                        dataset.sensor = \
                            metadatadict['n1:Level-1C_User_Product']['n1:General_Info']['Product_Info']['Datatake'][
                                'SPACECRAFT_NAME']
                        dataset.level = metadatadict['n1:Level-1C_User_Product']['n1:General_Info']['Product_Info'][
                            'PROCESSING_LEVEL']

                        daynight = 'day'
                        if metadatadict['n1:Level-1C_User_Product']['n1:General_Info']['Product_Info']['Datatake'][
                            'SENSING_ORBIT_DIRECTION'] != 'DESCENDING':
                            daynight = 'night'
                        dataset.daynight = daynight

                    quicklookurl = SENTINEL_S3_HTTP_BASEURL + tilenfodict['path'] + '/preview.jpg'
                    metadataurl = SENTINEL_S3_HTTP_BASEURL + productkey + '/metadata.xml'

                    container = dict()
                    if remote_file_exists(quicklookurl):
                        container['quicklook'] = quicklookurl
                    if remote_file_exists(metadataurl):
                        container['metadata'] = metadataurl
                    if remote_file_exists(SENTINEL_S3_HTTP_ZIP_BASEURL + dataset.entity_id + '.zip'):
                        s3.zip = SENTINEL_S3_HTTP_ZIP_BASEURL + dataset.entity_id + '.zip'
                    if s3.zip != None or s3.bucket != None:
                        container.update(s3.to_dict())

                    dataset.resources = container
                    datasets.append(dataset)

        print counter, 'processed...', N

    return datasets


def sentinel_harvester_line(lines):
    datasets = []
    for line in lines:
        content_list = line.split(' ')
        tileinfokey = content_list[-1]
        tileinfokey = tileinfokey.rstrip("\n")
        quicklookkey = tileinfokey.replace('tileInfo.json', 'preview.jpg')
        if public_key_exists(SENTINEL_S3_BUCKET, tileinfokey) and public_key_exists(SENTINEL_S3_BUCKET, quicklookkey):
            tilenfodict = ujson.loads(public_get_filestream(SENTINEL_S3_BUCKET, tileinfokey))
            productkey = tilenfodict['productPath']

            s3 = SentinelS3Container()
            s3.bucket = SENTINEL_S3_BUCKET
            s3.tile = tilenfodict['path'] + '/'
            s3.quicklook = quicklookkey

            dataset = Catalog_Dataset()
            dataset.entity_id = tilenfodict['productName']
            dataset.tile_identifier = '%02d%s%s' % (
            tilenfodict['utmZone'], tilenfodict['latitudeBand'], tilenfodict['gridSquare'])
            dataset.clouds = tilenfodict['cloudyPixelPercentage']
            dataset.acq_time = dateutil.parser.parse(tilenfodict['timestamp'])

            if public_key_exists(SENTINEL_S3_BUCKET, productkey + '/metadata.xml'):
                s3.product = productkey + '/'
                metadatakey = productkey + '/metadata.xml'
                metadatadict = xmltodict.parse(
                    public_get_filestream(SENTINEL_S3_BUCKET, metadatakey))

                dataset.sensor = \
                    metadatadict['n1:Level-1C_User_Product']['n1:General_Info']['Product_Info']['Datatake'][
                        'SPACECRAFT_NAME']
                dataset.level = metadatadict['n1:Level-1C_User_Product']['n1:General_Info']['Product_Info'][
                    'PROCESSING_LEVEL']

                daynight = 'day'
                if metadatadict['n1:Level-1C_User_Product']['n1:General_Info']['Product_Info']['Datatake'][
                    'SENSING_ORBIT_DIRECTION'] != 'DESCENDING':
                    daynight = 'night'
                dataset.daynight = daynight

            quicklookurl = SENTINEL_S3_HTTP_BASEURL + tilenfodict['path'] + '/preview.jpg'
            metadataurl = SENTINEL_S3_HTTP_BASEURL + productkey + '/metadata.xml'

            container = dict()
            if remote_file_exists(quicklookurl):
                container['quicklook'] = quicklookurl
            if remote_file_exists(metadataurl):
                container['metadata'] = metadataurl
            if remote_file_exists(SENTINEL_S3_HTTP_ZIP_BASEURL + dataset.entity_id + '.zip'):
                s3.zip = SENTINEL_S3_HTTP_ZIP_BASEURL + dataset.entity_id + '.zip'
            if s3.zip != None or s3.bucket != None:
                container.update(s3.to_dict())

            dataset.resources = container
            datasets.append(dataset)

    return datasets


def import_from_file_s2(in_csv, block_size):
    import pprint
    n, m = (0, block_size)
    api = Api()
    for n in range(0, count_lines(in_csv), m):
        print 'Range: <%d:%d>' % (n, n + m)
        datasets = sentinel_harvester(in_csv, n, m)
        out = api.create_dataset(datasets)
        pprint.pprint(out)


def import_from_pipe_s2(lines):
    import pprint
    api = Api()
    datasets = sentinel_harvester_line(lines)
    out = api.create_dataset(datasets)
    pprint.pprint(out)


def import_from_sentinel_catalog(sensor,start_date, api_url):
    import numpy
    api = Api(api_url)

    max_cloud_ratio = 1.0
    ag_season_start = dateutil.parser.parse(start_date)
    ag_season_end = ag_season_start + datetime.timedelta(days=1)

    for lon in numpy.arange(-180,180,9):
        for lat in numpy.arange(-90,90,9):
            lon_end = lon + 9
            lat_end = lat + 9

            aoi_se = (lon_end, lat)
            aoi_nw = (lon, lat_end)
            aoi_ne = (aoi_se[0], aoi_nw[1])
            aoi_sw = (aoi_nw[0], aoi_se[1])
            aoi = [aoi_nw, aoi_ne, aoi_se, aoi_sw, aoi_nw]

            cat = SentinelCatalog()
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
                print 'No data found in catalog for %s from %s to %s' % (
                sensor, ag_season_start.strftime("%Y-%m-%d"), ag_season_end.strftime("%Y-%m-%d"))
