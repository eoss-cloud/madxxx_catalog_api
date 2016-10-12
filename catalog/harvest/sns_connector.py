import json
import pprint
import sys
import time

import boto3
import botocore
import os
import requests
from api.eoss_api import Api
from harvest.sns_harvester import extract_s3_structure, parse_l1_metadata_file, get_message_type, generate_s2_tile_information
from utilities import chunks

MAX_MESSAGES = 10


def remove_messages_from_queue(message_list):
    for x in chunks(message_list, MAX_MESSAGES):
        print queue.delete_messages(
            Entries=x)
    return list()


if __name__ == '__main__':
    api = Api()
    # Get the service resource
    sqs = boto3.resource('sqs')
    print 'Found queues:'
    for queue in sqs.queues.all():
        print ' * %s' % queue
    if len(sys.argv) == 1:
        print 'Please specify queue name with command.'
        sys.exit()

    queue = sqs.get_queue_by_name(QueueName=sys.argv[1])
    pprint.pprint(queue.attributes)
    should_break = False
    counter = 1

    while not should_break:
        if int(queue.attributes.get('ApproximateNumberOfMessages')) == 0:
            time_interval = 60
        else:
            time_interval = 2
        print 'Getting messages from %s (%d sec. interval)' % (queue, time_interval)

        for message_obj in queue.receive_messages(MaxNumberOfMessages=MAX_MESSAGES, WaitTimeSeconds=10):
            messages_to_delete = list()
            notification = json.loads(message_obj.body)
            message = json.loads(notification[u'Message'])

            if get_message_type(message) == 'landsat':
                print 'Landsat mode'
                for rec in message[u'Records']:
                    s3 = extract_s3_structure(rec)
                    s3['metadata'] = os.path.join(s3['s3_http'], s3['s3_path'], s3['entity_id'] + '_MTL.txt')
                    s3['metadata_json'] = os.path.join(s3['s3_http'], s3['s3_path'], s3['entity_id'] + '_MTL.json')
                    s3['quicklook'] = os.path.join(s3['s3_http'], s3['s3_path'], s3['entity_id'] + '_thumb_large.jpg')
                    req = requests.get(s3['metadata_json'])

                    try:
                        obj = parse_l1_metadata_file(req.json(), s3)
                        print counter, obj.entity_id, api.catalog_put(obj)
                        counter += 1
                        messages_to_delete.append({
                            'Id': message_obj.message_id,
                            'ReceiptHandle': message_obj.receipt_handle
                        })
                    except ValueError:
                        print 'ERROR: metadata location structure corrupted'
                        print req.text
                    except Exception, e:
                        print 'ERROR:', e
                        should_break = True
                        if len(messages_to_delete) > 0:
                            messages_to_delete = remove_messages_from_queue(messages_to_delete)
            elif get_message_type(message) == 'sentinel2':
                print 'Sentinel2 mode'
                for tile in message[u'tiles']:
                    tile_path = tile[u'path']
                    obj = generate_s2_tile_information(tile_path)
                    if obj != None:
                        try:
                            print counter, obj.entity_id, api.catalog_put(obj)
                            counter += 1
                            messages_to_delete.append({
                                'Id': message_obj.message_id,
                                'ReceiptHandle': message_obj.receipt_handle
                            })
                        except Exception, e:
                            print 'ERROR:', e
                            should_break = True
                            if len(messages_to_delete) > 0:
                                messages_to_delete = remove_messages_from_queue(messages_to_delete)

                                # should_break = True

            pprint.pprint(messages_to_delete)
            messages_to_delete = list(messages_to_delete)
            if len(messages_to_delete) > 0:
                try:
                    messages_to_delete = remove_messages_from_queue(messages_to_delete)
                except botocore.exceptions.ClientError, e:
                    print e
                    print messages_to_delete

        time.sleep(time_interval)
