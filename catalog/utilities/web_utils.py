import logging

import boto3
import botocore
import requests
from botocore.handlers import disable_signing

logger = logging.getLogger()


def remote_file_exists(remote_url):
    """
    Check if remote url is accessible
    :param remote_url:  url string
    :return: True, if a request to the resource returns 200
    """

    req = requests.head(remote_url)
    if req.status_code in (requests.codes.ok, requests.codes.found):
        return True
    else:
        return False


def public_key_exists(bucket_name, prefix):
    """
    Check if public bucket exists and is accessible
    :param bucket_name:
    :param prefix:
    :return: True, if bucket resource can be accessed
    """
    s3 = boto3.resource('s3')
    s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)

    try:
        s3.Object(bucket_name, prefix).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == str(requests.codes.not_found):
            return False
        else:
            raise e
    else:
        return True


def public_get_filestream(bucket_name, prefix):
    s3 = boto3.resource('s3')
    s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)

    try:
        obj = s3.Object(bucket_name=bucket_name, key=prefix)
        response = obj.get()
        return response['Body'].read()
    except botocore.exceptions.ClientError as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == requests.codes.not_found:
            return None
        else:
            raise e
