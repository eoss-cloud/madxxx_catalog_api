import ujson
import pprint
import dateutil.parser
import os
import xmltodict
from model.plain_models import GoogleLandsatContainer, S3PublicContainer, Catalog_Dataset, SentinelS3Container
from utilities.web_utils import public_key_exists, public_get_filestream, remote_file_exists
import logging
import general.catalog_logger

SENTINEL_S3_HTTP_ZIP_BASEURL = 'http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/zips/'
SENTINEL_S3_HTTP_BASEURL = 'http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/'
SENTINEL_S3_BUCKET = 'sentinel-s2-l1c'

logger=logging.getLogger(__name__)

def make_catalog_entry(s, aws_struc):
    dataset = Catalog_Dataset()
    dataset.entity_id = s["METADATA_FILE_INFO"]["LANDSAT_SCENE_ID"]
    dataset.sensor = s["PRODUCT_METADATA"]["SENSOR_ID"]
    dataset.tile_identifier = '%03d%03d' % (int(s["PRODUCT_METADATA"]["WRS_PATH"]), int(s["PRODUCT_METADATA"]["WRS_ROW"]))
    dataset.clouds = float(s["IMAGE_ATTRIBUTES"]["CLOUD_COVER"])

    if int(s["PRODUCT_METADATA"]["WRS_PATH"]) > 0 and int(s["PRODUCT_METADATA"]["WRS_PATH"]) < 123:
        dataset.daynight = 'day'
    elif int(s["PRODUCT_METADATA"]["WRS_PATH"]) > 122 and int(s["PRODUCT_METADATA"]["WRS_PATH"]) < 234:
        dataset.daynight = 'night'
    date_str = '%sT%s' % (s["PRODUCT_METADATA"]["DATE_ACQUIRED"], s["PRODUCT_METADATA"]["SCENE_CENTER_TIME"])
    dataset.acq_time = dateutil.parser.parse(date_str)
    dataset.level = s["PRODUCT_METADATA"]["DATA_TYPE"]

    container = dict()
    container['quicklook'] = aws_struc['quicklook']
    container['metadata'] = aws_struc['metadata']

    google_sensors = GoogleLandsatContainer.supported_sensors

    google = GoogleLandsatContainer()
    google.link = google.base % (google_sensors[dataset.sensor],
                                 int(s["PRODUCT_METADATA"]["WRS_PATH"]),
                                 int(s["PRODUCT_METADATA"]["WRS_ROW"]),
                                 dataset.entity_id)
    container.update(google.to_dict())

    aws_s3 = S3PublicContainer()
    aws_s3.bucket = aws_struc['bucket_name']
    aws_s3.prefix = aws_struc['s3_path']
    aws_s3.http = aws_struc['s3_http']

    container.update(aws_s3.to_dict())
    dataset.resources = container

    return dataset


def parse_notification_json(filename):
    with open(filename, 'r') as f:
        return ujson.load(f)


def extract_s3_structure(record, type='landsat'):
    s3 = dict()
    if type == 'landsat':
        s3['aws_region'] = record[u'awsRegion']
        bucket = record[u's3'][u'bucket'][u'arn']
        s3['bucket_name'] = bucket[bucket.rfind(':') + 1:]
        s3['object'] = record[u's3'][u'object'][u'key']
        s3['s3_path'] = os.path.dirname(s3['object']) + '/'
        s3['s3_http'] = 'http://landsat-pds.s3.amazonaws.com/'
        s3['entity_id'] = os.path.dirname(s3['object']).split('/')[-1]
    elif type == 'sentinel2':
        pass

    return s3


def parse_l1_metadata_file(l1_dict, s3):

    return make_catalog_entry(l1_dict[u'L1_METADATA_FILE'], s3)


def main_landsat(message):
    for rec in message[u'Records']:
        pprint.pprint(rec)
        s3 = extract_s3_structure(rec)
        s3['metadata'] = os.path.join(s3['s3_path'], s3['entity_id'] + '_MTL.txt')
        s3['quicklook'] = os.path.join(s3['s3_path'], s3['entity_id'] + '_thumb_large.jpg')
        pprint.pprint(s3)
        # for f in [s3['object'], os.path.join(s3['s3_path'], s3['entity_id'] + '_MTL.json')]:
        #    print get_public_bucket_resource(s3['bucket_name'], f, out_dir, s3['aws_region'])
        f = os.path.join(out_dir, 'LC80290202016250LGN00_MTL.json')
        obj = parse_l1_metadata_file(ujson.load(open(f, 'rt')), s3)
        print obj.__dict__
    return obj


def get_message_type(message):
    if u'Records' in message:
        return 'landsat'
    elif u'sciHubIngestion' in message:
        return 'sentinel2'


def generate_s2_tile_information(tile_path):
    tileinfokey = os.path.join(tile_path, 'tileInfo.json')
    quicklookkey = os.path.join(tile_path, 'preview.jpg')

    if public_key_exists(SENTINEL_S3_BUCKET, tileinfokey) and public_key_exists(SENTINEL_S3_BUCKET, quicklookkey):
        tilenfodict = ujson.loads(public_get_filestream(SENTINEL_S3_BUCKET, tileinfokey))
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
        return dataset
        logger.warn( "WARNING: Metadata does not exist for %s" % (tile_path))
    return None
