# ========================
# Helpers for s3 
# ========================

import collections
from lib.sys_params import SYS_PARAMS

import boto3
import botocore

import lib.s3_helpers
from lib.common_helpers import get_full_download_path

def get_s3_resource():
    """
    get s3 resource for further process

    Args:
        None

    Return:
        object => s3 resource
    """

    # init the boto3 session
    session = boto3.session.Session()
    return session.resource('s3')

def download_file_to_tmp(bucket, file_name, file_key):
    """
    download file from s3 bucket to a temporary folder for further process

    Args:
        bucket 
            string => bucket name
        file_name
            string => target bucket
        file_key: 
            string => object key from s3

    Return:
        None
    """

    # get s3 client
    s3 = get_s3_resource()

    try:    
        s3.Object(bucket, file_key).download_file(get_full_download_path(file_name))
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            print(e)
            raise

def upload_json_file(bucket, key, body):
    """
    upload json file to a certain bucket

    Args:
        bucket
            string => target bucket
        key: 
            string => object key
        body:
            bytes => json content in bytes

    Return:
        None
    """

    # get s3 client
    s3 = get_s3_resource()
    
    try:    
        reponse = s3.meta.client.put_object(Bucket=bucket, Key=key, Body=body)
        print(reponse)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            print(e)
            raise
    except Exception as e:
        print(e)
        raise

def split_file_name(key):
    """
    split s3 object key to session_id and file name

    Args:
        key: 
            string => object key

    Return:
        string =>  session id, file name
    """

    splitted = key.split('/')
    return splitted[1], splitted[-1]

def obtain_object_tags_from_s3(key):
    """
    split s3 object tags into an ordered dictionary

    Args:
        key: 
            string => object key

    Return:
        dict => tags in the ordered dictionary
    """

    # get s3 client
    s3 = get_s3_resource()

    tag_dict = collections.OrderedDict()

    try:    
        tags = s3.meta.client.get_object_tagging(Bucket=SYS_PARAMS.SRC_BUCKET, Key=key)['TagSet']
        for tag in tags:
            tag_dict.update({tag.setdefault('Key', 'NULL'): tag.setdefault('Value', 'NULL')})

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")

    return tag_dict
