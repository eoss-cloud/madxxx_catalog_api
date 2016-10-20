import click
import boto3
import sys

from harvest.initial_harverster_ls import import_from_file_ls, import_from_pipe_ls, \
    import_from_landsat_catalog
from harvest.initial_harverster import import_from_file_s2, import_from_pipe_s2, \
    import_from_sentinel_catalog
from harvest.sns_connector import list_queues, update_catalog


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version='1.0.0')
def cli(*args, **kwargs):
    """
    EOSS catalog
    SNS connector
    update catalog with external sources (SQS, catalog providers)
    """


@cli.command('queue_update', short_help='update catalog with SQS queue')
@click.argument('queue_name', nargs=1)
@click.argument('api_endpoint', nargs=1, required=False,  default='http://api.eoss.cloud')
def update_catalog_queue(queue_name, api_endpoint):
    update_catalog(queue_name, api_endpoint)


@cli.command('queue_list', short_help='list all available queues')
def list_available_queues():
    list_queues()


@click.argument('block_size', nargs=1)
@click.argument('filename', nargs=1)
@cli.command('file_import_landsat', short_help='update catalog with exported landsat metadata file')
def file(filename, block_size):
    import_from_file_ls(filename, block_size)


@cli.command('pipe import landsat', short_help='update catalog with landsat pipe')
def pipe():
    lines = list()
    for line in sys.stdin:
        lines.append(line.replace("\n", ""))

    import_from_pipe_ls(lines)


@click.argument('block_size', nargs=1)
@click.argument('filename', nargs=1)
@cli.command('file_import_sentinel2', short_help='update catalog with exported sentinel2 metadata file')
def file(filename, block_size):
    import_from_file_s2(filename, block_size)


@cli.command('pipe import sentinel2', short_help='update catalog with sentinel2 pipe')
def pipe():
    lines = list()
    for line in sys.stdin:
        lines.append(line.replace("\n", ""))

    import_from_pipe_s2(lines)

@cli.command('catalog_import', short_help='update catalog with provider catalog queries')
@click.argument('sensor', nargs=1)
@click.argument('start_date', nargs=1)
@click.argument('api_endpoint', nargs=1, required=False,  default='http://api.eoss.cloud')
def synchronize_catalog(sensor, start_date, api_endpoint):
    if sensor == 'sentinel2':
        import_from_sentinel_catalog(sensor,start_date, api_endpoint)
    elif sensor == 'landsat8':
        import_from_landsat_catalog( "LANDSAT_8",start_date, api_endpoint) # "LANDSAT_8", "LANDSAT_ETM_SLC_OFF", "LANDSAT_ETM"
    elif sensor == 'landsat7':
        import_from_landsat_catalog("LANDSAT_ETM", start_date, api_endpoint)  # "LANDSAT_8", "LANDSAT_ETM_SLC_OFF", "LANDSAT_ETM"
    elif sensor == 'landsat7off':
        import_from_landsat_catalog("LANDSAT_ETM_SLC_OFF", start_date, api_endpoint)  # "LANDSAT_8", "LANDSAT_ETM_SLC_OFF", "LANDSAT_ETM"

if __name__ == '__main__':
    cli()

