#!/usr/bin/env python
# coding: utf8
# Created by sgebhardt at 30.08.16
# Copyright EOSS GmbH 2016
import json
import sys

import dateutil.parser
import xmltodict
from api.eoss_api import Api
from manage.sentinelcatalog import SENTINEL_S3_BUCKET, SENTINEL_S3_HTTP_ZIP_BASEURL, \
    SENTINEL_S3_HTTP_BASEURL
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
                if public_key_exists(SENTINEL_S3_BUCKET, tileinfokey) and public_key_exists(SENTINEL_S3_BUCKET, quicklookkey):
                    tilenfodict = json.loads(public_get_filestream(SENTINEL_S3_BUCKET, tileinfokey))
                    productkey = tilenfodict['productPath']

                    s3 = SentinelS3Container()
                    s3.bucket = SENTINEL_S3_BUCKET
                    s3.tile = tilenfodict['path'] + '/'
                    s3.quicklook = quicklookkey

                    dataset = Catalog_Dataset()
                    dataset.entity_id = tilenfodict['productName']
                    dataset.tile_identifier = '%02d%s%s' % (tilenfodict['utmZone'], tilenfodict['latitudeBand'], tilenfodict['gridSquare'])
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


def sentinel_harvester_line(line):
    datasets = []
    for line in lines:
        content_list = line.split(' ')
        tileinfokey = content_list[-1]
        tileinfokey = tileinfokey.rstrip("\n")
        quicklookkey = tileinfokey.replace('tileInfo.json', 'preview.jpg')
        if public_key_exists(SENTINEL_S3_BUCKET, tileinfokey) and public_key_exists(SENTINEL_S3_BUCKET, quicklookkey):
            tilenfodict = json.loads(public_get_filestream(SENTINEL_S3_BUCKET, tileinfokey))
            productkey = tilenfodict['productPath']

            s3 = SentinelS3Container()
            s3.bucket = SENTINEL_S3_BUCKET
            s3.tile = tilenfodict['path'] + '/'
            s3.quicklook = quicklookkey

            dataset = Catalog_Dataset()
            dataset.entity_id = tilenfodict['productName']
            dataset.tile_identifier = '%02d%s%s' % (tilenfodict['utmZone'], tilenfodict['latitudeBand'], tilenfodict['gridSquare'])
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


def main_s2(in_csv):
    import pprint
    n, m = (0, 5)
    api = Api()
    for n in range(0, 10000, m):
        print 'Range: <%d:%d>' % (n, n + m)
        datasets = sentinel_harvester(in_csv, n, m)
        out = api.catalog_put(datasets)
        pprint.pprint(out)


def main_s2a(lines):
    import pprint
    api = Api()
    datasets = sentinel_harvester_line(lines)
    out = api.catalog_put(datasets)
    pprint.pprint(out)


if __name__ == '__main__':
    lines = list()
    for line in sys.stdin:
        lines.append(line.replace("\n", ""))

    print "Executing harvester with %d lines ..." % len(lines)
    # in_csv = '/Users/wehrmann/eoss/temp/s2_list32.txt'
    main_s2a(lines)
