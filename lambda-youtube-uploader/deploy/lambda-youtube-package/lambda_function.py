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

import pytz

def search_list_by_keyword(client, **kwargs):
  # See full sample for function
  kwargs = remove_empty_kwargs(**kwargs)

  response = client.search().list(
    **kwargs
  ).execute()

  return response

def check_if_video_exist(file_name, date_time_original, projectId, site, subSite, cameraLocation):
    """
    check if video exists in TaiBIF

    Args:
        file_name: 
            string => the status of this action
        original_datetime: 
            string =>  video original datetime 
        projectId: 
            string =>  from object tag - projectId
        site: 
            string =>  from object tag - site
        subSite: 
            string =>  from object tag - sub_subsite
        cameraLocation: 
            string =>  from object tag - cameraLocation

    Return:
        bool, string => True if video exists , url 
    """

    is_video_exist = False
    youtube_url = ''
    youtube_playlist_id = ''

    # check if this video has been uploaded or not
    # if the video was already uploaded, then dismiss the job
    location_path = CommenHelpers.generate_location_path(projectId, site, subSite, cameraLocation)
    location_path_md5 = CommenHelpers.to_md5_hexdigest(location_path)

    result = query_multimedia_metadata(file_name, int(pytz.timezone('Asia/Taipei').localize(date_time_original).timestamp()), location_path_md5)

    if 'results' in result and result['results'] is not None and len(result['results']) > 0 and 'youtube_url' in result['results'][0] and result['results'][0]['youtube_url'] is not None:
        is_video_exist = True
        youtube_url = result['results'][0]['youtube_url']
        youtube_playlist_id = result['results'][0]['youtube_playlist_id']
    
    return is_video_exist, youtube_url, youtube_playlist_id

def set_default_value(target_dict):
    """
    set certain pairs to default NULL if the pairs don't exist or have empty value 

    Args:
        target_dict: 
            dict => target dictionary

    Return:
        None
    """

    target_dict.setdefault('projectId', 'NULL')
    target_dict.setdefault('projectTitle', 'NULL')
    target_dict.setdefault('site', 'NULL')
    target_dict.setdefault('subSite', 'NULL')
    target_dict.setdefault('cameraLocation', 'NULL')
    target_dict.setdefault('userId', 'NULL')

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
    # is_video_exist, youtube_url, youtube_playlist_id = check_if_video_exist(file_name, 
    #                                        video_meta['date_time_original'], 
    #                                        tags['projectId'], 
    #                                        tags['site'], 
    #                                        tags['subSite'], 
    #                                        tags['cameraLocation'])

    try:
        # get authorization
        client_instance = get_authenticated_service()

        location_path = CommenHelpers.generate_location_path(tags['projectId'], tags['site'], tags['subSite'], tags['cameraLocation'])
        relocate_path = 'video/orig/{}'.format(location_path)
        file_parts = file_name.split('.')
        ext = file_parts.pop()
        date_time_original_timestamp = int(pytz.timezone('Asia/Taipei').localize(video_meta['date_time_original']).timestamp()) # 由 date_time_original 轉換而來
        base_file_name = '{}_{}.{}'.format('.'.join(file_parts), date_time_original_timestamp, ext.lower())
        relative_url = '{}/{}'.format(relocate_path, base_file_name)
        url_md5 = CommenHelpers.to_md5_hexdigest(relative_url)
        print(relative_url)
        
        found = search_list_by_keyword(client_instance,
            part='snippet',
            maxResults=1,
            forMine=1,
            q=url_md5,
            type='video')

        upload_meta = [tags['projectId'], tags['projectTitle'], tags['site'], tags['subSite'], tags['cameraLocation'], url_md5]

        # if is_video_exist:
        if found['pageInfo']['totalResults'] > 0:
            if found['items'][0]['snippet']['title'] == url_md5:
                video_id = found['items'][0]['id']['videoId']
                print('{} was already uploaded. url: {}'.format(file_name, video_id))
           
        else:
            # upload video
            args.title = url_md5
            video_id = initialize_upload(client_instance, args)

        # add video to target playlist
        youtube_url = '{}{}'.format(SYS_PARAMS.YOUTUBE_VIDEO_URL, video_id)

        youtube_playlist_id = add_video_to_playlist(client_instance, video_id, upload_meta)

        # create mma/mmm json file and upload to s3 bucket
        json_gen = JsonFileGenerator(bucket=SYS_PARAMS.SRC_BUCKET,
                                    youtube_url=youtube_url,
                                    youtube_playlist_id=youtube_playlist_id,
                                    projectId=tags['projectId'],
                                    projectTitle=tags['projectTitle'],
                                    site=tags['site'],
                                    subSite=tags['subSite'],
                                    cameraLocation=tags['cameraLocation'],
                                    video_name=file_name,
                                    video_length=video_meta['duration'],
                                    video_org_datetime=video_meta['date_time_original'],
                                    video_mod_datetime=video_meta['date_last_modification'],
                                    video_width=video_meta['width'],
                                    video_height=video_meta['height'],
                                    userId=tags['userId'],
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
