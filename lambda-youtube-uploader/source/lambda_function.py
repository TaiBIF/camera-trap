import argparse
import json
from lib.sys_params import SYS_PARAMS
import urllib

import boto3
import botocore
from googleapiclient.errors import HttpError

import lib.s3_helpers as S3Helpers
import lib.common_helpers as CommenHelpers
from lib.extract_video_meta import extra_video_meta
from lib.json_file_generator import JsonFileGenerator
from lib.taibif_api import query_multimedia_metadata
from lib.upload_video import *

def check_if_video_exist(file_name, date_time_original, project, site, sub_site, location):
    """
    check if video exists in TaiBIF

    Args:
        file_name: 
            string => the status of this action
        original_datetime: 
            string =>  video original datetime 
        project: 
            string =>  from object tag - project
        site: 
            string =>  from object tag - site
        sub_site: 
            string =>  from object tag - sub_subsite
        location: 
            string =>  from object tag - location

    Return:
        bool, string => True if video exists , url 
    """

    is_video_exist = False
    url = ''

    # check if this video has been uploaded or not
    # if the video was already uploaded, then dismiss the job
    location_path = CommenHelpers.generate_location_path(project, site, sub_site, location)
    result = query_multimedia_metadata(file_name, int(date_time_original.timestamp()), CommenHelpers.to_md5_hexdigest(location_path))

    if 'results' in result and result['results'] is not None and len(result['results']) > 0:
        is_video_exist = True
        url = result['results'][0]['url']
    
    return is_video_exist, url

def set_default_value(target_dict):
    """
    set certain pairs to default NULL if the pairs don't exist or have empty value 

    Args:
        target_dict: 
            dict => target dictionary

    Return:
        None
    """

    target_dict.setdefault('project', 'NULL')
    target_dict.setdefault('site', 'NULL')
    target_dict.setdefault('sub_site', 'NULL')
    target_dict.setdefault('location', 'NULL')
    target_dict.setdefault('user_id', 'NULL')

def lambda_handler(event, context):  
    print('event: {}'.format(event))
    
    event_key = urllib.parse.unquote(event['Records'][0]['s3']['object']['key'])
    session_id, file_name = S3Helpers.split_file_name(event_key)
    tags = S3Helpers.obtain_object_tags_from_s3(event_key)
    set_default_value(tags)

    print('session_id: {}'.format(session_id))
    print('file_name: {}'.format(file_name))
    print('tags: {}'.format(tags))

    parser = argparse.ArgumentParser()

    # path of the file location
    parser.add_argument('--file', default=CommenHelpers.get_full_download_path(file_name))

    # video title on YouTube
    parser.add_argument('--title', default=file_name)

    # description for the video
    parser.add_argument('--description', default=file_name)

    # default 27 - Education, see more - https://developers.google.com/youtube/v3/docs/videoCategories/list
    parser.add_argument('--category', default='27') 

    # keywords for the video
    parser.add_argument('--keywords', default=tags)

    # set if this video is public or private. options: 'public', 'private', 'unlisted'
    parser.add_argument('--privacyStatus', default='public')
    args = parser.parse_args()

    # download file to /tmp
    S3Helpers.download_file_to_tmp(SYS_PARAMS.SRC_BUCKET, file_name, event_key)

    # get video metadata
    video_meta = extra_video_meta(CommenHelpers.get_full_download_path(file_name))

    # check if this video has been uploaded or not
    # if the video was already uploaded, then dismiss the job
    is_video_exist, youtube_url = check_if_video_exist(file_name, 
                                            video_meta['date_time_original'], 
                                            tags['project'], 
                                            tags['site'], 
                                            tags['sub_site'], 
                                            tags['location'])
    if is_video_exist:
        print('{} was already uploaded. url: {}'.format(file_name, youtube_url))
    else:
        # get authorization
        client_instance = get_authenticated_service()

        try:
            # upload video
            video_id = initialize_upload(client_instance, args)
        
            # add video to target playlist
            playlist_id = add_video_to_playlist(client_instance, video_id, tags['location'])

            # create mma/mmm json file and upload to s3 bucket
            json_gen = JsonFileGenerator(bucket=SYS_PARAMS.SRC_BUCKET,
                                        youtube_url='{}{}'.format(SYS_PARAMS.YOUTUBE_VIDEO_URL, video_id),
                                        youtube_playlist_id=playlist_id,
                                        project=tags['project'],
                                        site=tags['site'],
                                        sub_site=tags['sub_site'],
                                        location=tags['location'],
                                        video_name=file_name,
                                        video_length=video_meta['duration'],
                                        video_org_datetime=video_meta['date_time_original'],
                                        video_mod_datetime=video_meta['date_last_modification'],
                                        video_width=video_meta['width'],
                                        video_height=video_meta['height'],
                                        user_id=tags['user_id'],
                                        upload_session_id=session_id,
                                        device_metadata=video_meta['device_metadata'],
                                        exif=video_meta['exif'], 
                                        make=video_meta['make'],
                                        model=video_meta['model'])

            json_gen.do_process()

        except HttpError as e:
            print('An HTTP error %d occurred:\n%s' % (e.resp.status, e.content))

        except Exception as e:
            print(e)

    return {
        "statusCode": 200,
        "body": json.dumps('Success')
    }
