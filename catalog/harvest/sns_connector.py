import ujson
import pprint
import sys
import time
import click
import boto3
import botocore
import os
import requests
from api.eoss_api import Api
from harvest.sns_harvester import extract_s3_structure, parse_l1_metadata_file, get_message_type, generate_s2_tile_information
from utilities import chunks
import general.catalog_logger
import logging

MAX_MESSAGES = 10

logger=logging.getLogger(__name__)

# Get the service resource
sqs = boto3.resource('sqs')


def remove_messages_from_queue(queue, message_list):
    for x in chunks(message_list, MAX_MESSAGES):
        try:
            queue.delete_messages(Entries=x)
        except botocore.exceptions.ClientError, e:
            logger.error('Error occured during clean up queue: %s'%str(e))
    logger.info('Removing %d from %s' % (len(message_list), queue))
    return list()


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(version='1.0.0')
def cli(*args, **kwargs):
    """
    EOSS catalog
    SNS connector
    update catalog with SQS notifications
    """


def get_all_queues():
    queue_names = list()
    for q in sqs.queues.all():
        queue_names.append( q.url[q.url.rfind('/') + 1:])

    return queue_names


@cli.command()
@click.argument('queue_name', nargs=1)
def update_catalog(queue_name):
    api = Api()

    if queue_name not in get_all_queues():
        raise Exception('Queue %s does not exist in %s' % (queue_name, get_all_queues()))
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    should_break = False
    counter = 1

    while not should_break:
        if int(queue.attributes.get('ApproximateNumberOfMessages')) == 0:
            time_interval = 60
        else:
            time_interval = 2
        logger.info('Getting messages from SQS: %s (%d sec. interval)' % (queue_name, time_interval))

        for message_obj in queue.receive_messages(MaxNumberOfMessages=MAX_MESSAGES, WaitTimeSeconds=10):
            messages_to_delete = list()
            notification = ujson.loads(message_obj.body)
            message = ujson.loads(notification[u'Message'])

            if get_message_type(message) == 'landsat':
                for rec in message[u'Records']:
                    s3 = extract_s3_structure(rec)
                    s3['metadata'] = os.path.join(s3['s3_http'], s3['s3_path'], s3['entity_id'] + '_MTL.txt')
                    s3['metadata_json'] = os.path.join(s3['s3_http'], s3['s3_path'], s3['entity_id'] + '_MTL.json')
                    s3['quicklook'] = os.path.join(s3['s3_http'], s3['s3_path'], s3['entity_id'] + '_thumb_large.jpg')
                    req = requests.get(s3['metadata_json'])

                    try:
                        obj = parse_l1_metadata_file(req.json(), s3)
                        print counter, len(messages_to_delete), int(queue.attributes.get('ApproximateNumberOfMessages'))
                        new_ds = api.create_dataset(obj)
                        if not new_ds is None:
                            print new_ds
                        counter += 1
                        messages_to_delete.append({
                            'Id': message_obj.message_id,
                            'ReceiptHandle': message_obj.receipt_handle
                        })
                    except ValueError:
                        logger.error('ERROR: metadata location structure corrupted')
                        print req.text
                    except Exception, e:
                        logging.exception('General Error ooccured')
                        should_break = True
                        if len(messages_to_delete) > 0:
                            messages_to_delete = remove_messages_from_queue(queue, messages_to_delete)
            elif get_message_type(message) == 'sentinel2':
                for tile in message[u'tiles']:
                    tile_path = tile[u'path']
                    obj = generate_s2_tile_information(tile_path)
                    if obj != None:
                        try:
                            print counter, len(messages_to_delete), int(queue.attributes.get('ApproximateNumberOfMessages'))
                            new_ds =  api.create_dataset(obj)
                            if not new_ds is None:
                                print new_ds
                            counter += 1
                            messages_to_delete.append({
                                'Id': message_obj.message_id,
                                'ReceiptHandle': message_obj.receipt_handle
                            })
                        except Exception, e:
                            logging.exception('General Error ooccured:')
                            should_break = True
                            if len(messages_to_delete) > 0:
                                messages_to_delete = remove_messages_from_queue(queue, messages_to_delete)

                                # should_break = True

            if len(messages_to_delete) > 0:
                try:
                    messages_to_delete = remove_messages_from_queue(queue, messages_to_delete)
                except botocore.exceptions.ClientError, e:
                    logger.exception('Error during removing processes messages in queue')

        time.sleep(time_interval)


@cli.command()
def list_queues():
    print 'Found queues:'
    for q in get_all_queues():
        queue = sqs.get_queue_by_name(QueueName=q)
        print ' * %s (%d) at %s' % (q, int(queue.attributes.get('ApproximateNumberOfMessages')), q.url)


if __name__ == '__main__':
    cli()

