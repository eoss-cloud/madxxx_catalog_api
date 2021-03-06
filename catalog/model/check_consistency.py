import operator

from api.eoss_api import Api
from dateutil.parser import parse
import datetime
import requests, grequests
import time
import logging
import click

from utilities import chunks

logger = logging.getLogger()


def append_data(file, data):
    with open(file,  "a") as myfile:
        myfile.writelines(data)


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version='1.0.0')
def cli(*args, **kwargs):
    """
    EOSS catalog
    consistency checker
    check if registered external URLs exist (e.g. quicklooks, metadata or zip archives
    """


@click.option('--api_endpoint', nargs=1, default='http://api.eoss.cloud')
@click.argument('sensor', nargs=1)
@click.argument('start_date', nargs=1, type=click.STRING)
@click.argument('days', nargs=1, type=click.INT)
@cli.command('check_consistency', short_help='update catalog with exported sentinel2 metadata file')
def main(sensor, start_date, days, api_endpoint):
    api = Api(api_endpoint)
    logger.info('Checking consistencty for %s between %s + %s' % (sensor, start_date, days))

    aoi_nw = (-180, 90)
    aoi_se = (180, -90)
    aoi_ne = (aoi_se[0], aoi_nw[1])
    aoi_sw = (aoi_nw[0], aoi_se[1])
    aoi = [aoi_nw, aoi_ne, aoi_se, aoi_sw, aoi_nw]
    wrong_urls = list()

    for delta_day in range(1, days):
        start_time = time.time()
        start_date_date = parse(start_date)+ datetime.timedelta(days=delta_day)
        end_date_date = start_date_date + datetime.timedelta(days=1)
        logger.info('Checking consistencty for %s between %s and %s' % (sensor, start_date_date.isoformat(), end_date_date.isoformat()))

        # Object representation
        results = api.search_dataset(aoi, 100, start_date_date, end_date_date, sensor, full_objects=False)

        url_resources = list()
        missing_urls = list()
        missing_types = list()

        for r in results:
            if r['resources']['s3public']['zip'] !=  None:
                url_resources.append(r['resources']['s3public']['zip'])
            else:
                missing_urls.append('%s:%s' % (r['tile_identifier'], r['entity_id']))
                missing_types.append('zip')
            if r['resources']['metadata']!=  None:
                url_resources.append(r['resources']['metadata'])
            else:
                missing_urls.append('%s:%s' % (r['tile_identifier'], r['entity_id']))
                missing_types.append('metadata')
            if r['resources']['quicklook'] != None:
                url_resources.append(r['resources']['quicklook'])
            else:
                missing_urls.append('%s:%s' % (r['tile_identifier'], r['entity_id']))
                missing_types.append('quicklook')


        logger.info('total scans: %d' %len(url_resources))
        logger.info('already missed resources: %d' %len(missing_urls))

        if False:
            for counter, res in enumerate(url_resources):
                req = requests.head(res)
                if req.status_code != requests.codes.ok:
                    print res, req.status_code
                    missing_urls.append(res)
                print res
                if (counter % 25) == 0:
                    print counter
        else:
            counter = 0
            for url_parts in chunks(url_resources, 500):
                counter+=1
                rs = (grequests.head(u) for u in url_parts)
                res = grequests.map(rs)
                for req in res:
                    if req is not None:
                        if req.status_code != requests.codes.ok:
                            wrong_urls.append(res)
                            missing_types.append('zip_registered')
                    else:
                        print req.url, req

        if len(wrong_urls) > 0:
            for item in wrong_urls:
                print item
                for req in item:
                    if req.status_code != requests.codes.ok:
                        append_data('/tmp/wrong_urls.txt', req.url)
        if len(missing_urls) > 0:
            append_data('/tmp/missing_urls.txt', missing_urls)

        if len(missing_types) > 0:
            for type in ['zip_registered', 'quicklook', 'metadata', 'zip']:
                logger.info('%d:%s' % (operator.countOf(missing_types, type), type))

        logger.info('wrong resources resources: %d' % len(wrong_urls))
        logger.info('Executed in %f secs.' % (time.time()-start_time))
    print 'Wrong URLs:', wrong_urls

if __name__ == '__main__':
    cli()

