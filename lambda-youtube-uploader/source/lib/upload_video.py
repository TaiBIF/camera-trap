# =====================================================
# Module that use YouTube APIs to do further processes
# =====================================================

import argparse
import http.client
import httplib2
import random
import time

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from oauth2client import client, GOOGLE_TOKEN_URI
from lib.sys_params import SYS_PARAMS

# ===========================
#        Properties
# ===========================

# Explicitly tell the underlying HTTP transport library not to retry, since
# we are handling retry logic ourselves.
httplib2.RETRIES = 1

# Maximum number of times to retry before giving up.
MAX_RETRIES = 10

# Always retry when these exceptions are raised.
RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError, http.client.NotConnected,
                        http.client.IncompleteRead, http.client.ImproperConnectionState,
                        http.client.CannotSendRequest, http.client.CannotSendHeader,
                        http.client.ResponseNotReady, http.client.BadStatusLine)

# Always retry when an apiclient.errors.HttpError with one of these status
# codes is raised.
RETRIABLE_STATUS_CODES = [500, 502, 503, 504]

# This OAuth 2.0 access scope allows an application to upload files to the
# authenticated user's YouTube channel, but doesn't allow other types of access.
SCOPES = ['https://www.googleapis.com/auth/youtube.upload']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

# ===========================
#        Commen usage
# ===========================

def get_authenticated_service():
    """
    authorize the request and store authorization credentials.

    Args:
        None

    Return:
        resource => api resource
    """

    credentials = client.OAuth2Credentials(access_token=None,
                                           client_id=SYS_PARAMS.CLIENT_ID,
                                           client_secret=SYS_PARAMS.CLIENT_SECRET,
                                           refresh_token=SYS_PARAMS.REFRESH_TOKEN,
                                           token_expiry=None,
                                           token_uri=GOOGLE_TOKEN_URI,
                                           user_agent=None,
                                           revoke_uri=None)

    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)

def build_resource(properties):
    """
    build a resource based on a list of properties given as key-value pairs.
    leave properties with empty values out of the inserted resource.

    Args:
        :properties
            dict => properties for building resources

    Return:
        dict => resource
    """

    resource = {}
    for p in properties:
        # Given a key like "snippet.title", split into "snippet" and "title", where
        # "snippet" will be an object and "title" will be a property in that object.
        prop_array = p.split('.')
        ref = resource
        for pa in range(0, len(prop_array)):
            is_array = False
            key = prop_array[pa]

            # For properties that have array values, convert a name like
            # "snippet.tags[]" to snippet.tags, and set a flag to handle
            # the value as an array.
            if key[-2:] == '[]':
                key = key[0:len(key)-2:]
                is_array = True

            if pa == (len(prop_array) - 1):
                # Leave properties without values out of inserted resource.
                if properties[p]:
                    if is_array:
                        ref[key] = properties[p].split(',')
                    else:
                        ref[key] = properties[p]
            elif key not in ref:
                # For example, the property is "snippet.title", but the resource does
                # not yet have a "snippet" object. Create the snippet object here.
                # Setting "ref = ref[key]" means that in the next time through the
                # "for pa in range ..." loop, we will be setting a property in the
                # resource's "snippet" object.
                ref[key] = {}
                ref = ref[key]
            else:
                # For example, the property is "snippet.description", and the resource
                # already has a "snippet" object.
                ref = ref[key]
    return resource

def remove_empty_kwargs(**kwargs):
    """
    remove keyword arguments that are not set

    Args:
        :**kwargs

    Return:
        dict => kwargs without empty attributes
    """

    good_kwargs = {}

    if kwargs is not None:
        for key, value in kwargs.items():
            if value:
                good_kwargs[key] = value
    return good_kwargs


# ===========================
#           Video
# ===========================

def initialize_upload(client_instance, options):
    """
    initiate the upload process

    Args:
        :client_instance
            resource => youtube resource

        :options
            objects

    Return:
        int => return yotube video id if success
    """

    tags = None
    if options.keywords and len(options.keywords) > 0:
        tags = list(options.keywords.values())

    body = dict(
        snippet=dict(
            title=options.title,
            description=options.description,
            tags=tags,
            categoryId=options.category
        ),
        status=dict(
            privacyStatus=options.privacyStatus
        )
    )

    # Call the API's videos.insert method to create and upload the video.
    insert_request = client_instance.videos().insert(
        part=','.join(body.keys()),
        body=body,
        # The chunksize parameter specifies the size of each chunk of data, in
        # bytes, that will be uploaded at a time. Set a higher value for
        # reliable connections as fewer chunks lead to faster uploads. Set a lower
        # value for better recovery on less reliable connections.
        #
        # Setting 'chunksize' equal to -1 in the code below means that the entire
        # file will be uploaded in a single HTTP request. (If the upload fails,
        # it will still be retried where it left off.) This is usually a best
        # practice, but if you're using Python older than 2.6 or if you're
        # running on App Engine, you should set the chunksize to something like
        # 1024 * 1024 (1 megabyte).
        media_body=MediaFileUpload(options.file, chunksize=-1, resumable=True)
    )

    video_id = resumable_upload(insert_request)
    return video_id

def resumable_upload(request):
    """
    this method implements an exponential backoff strategy to resume a failed upload.

    Args:
        :request
            object

    Return:
        int => return yotube video id if success
    """

    response = None
    error = None
    retry = 0
    video_id = None

    while response is None:
        try:
            print('Uploading file...')
            status, response = request.next_chunk()
            if response is not None:
                if 'id' in response:
                    print('Video id "%s" was successfully uploaded.' %
                          response['id'])
                    video_id = response['id']
                else:
                    exit('The upload failed with an unexpected response: %s' % response)
        except HttpError as e:
            if e.resp.status in RETRIABLE_STATUS_CODES:
                error = 'A retriable HTTP error %d occurred:\n%s' % (e.resp.status,
                                                                     e.content)
                print(e)
            else:
                raise
        except RETRIABLE_EXCEPTIONS as e:
            error = ('A retriable error occurred: %s' % e)
            print(e)

        if error is not None:
            print(error)
            retry += 1
            if retry > MAX_RETRIES:
                exit('No longer attempting to retry.')

            max_sleep = 2 ** retry
            sleep_seconds = random.random() * max_sleep
            print('Sleeping %f seconds and then retrying...' % sleep_seconds)
            time.sleep(sleep_seconds)

    return video_id

# ===========================
#          Playlist
# ===========================


def playlists_list_mine(client_instance, **kwargs):
    """
    api - get youtube playlist of mine

    Args:
        :client_instance
            resource => youtube resource
        :**kwargs

    Return:
        object => playlist result
    """

    # See full sample for function
    kwargs = remove_empty_kwargs(**kwargs)

    response = client_instance.playlists().list(**kwargs).execute()

    return response


def playlist_items_insert(client_instance, properties, **kwargs):
    """
    api - insert a video to playlist

    Args:
        :client_instance
            resource => youtube resource
        :properties
        :**kwargs

    Return:
        bool => return True if success
    """

    # See full sample for function
    resource = build_resource(properties)

    # See full sample for function
    kwargs = remove_empty_kwargs(**kwargs)

    try:
        client_instance.playlistItems().insert(
            body=resource,
            **kwargs
        ).execute()
        return True
    except Exception as e:
        print(e)
        return False


def playlists_insert(client_instance, properties, **kwargs):
    """
    api - create a playlist

    Args:
        :client_instance
            resource => youtube resource
        :properties
        :**kwargs

    Return:
        int => return the playlist id if the playlist has been created successfully
    """

    # See full sample for function
    resource = build_resource(properties)

    # See full sample for function
    kwargs = remove_empty_kwargs(**kwargs)

    try:
        response = client_instance.playlists().insert(
            body=resource,
            **kwargs
        ).execute()

        print(response)
        return response['id']

    except Exception as e:
        print(e)
        return None


def search_target_playlist(client_instance, key, next_page_token):
    """
    search if the playlist exists or not

    Args:
        :client_instance
            resource => youtube resource
        :key
            string => the title of playlist
        :next_page_token
            string => give this token to fetch the next page of the result list

    Return:
        int => return the playlist id if the playlist exists
    """

    # find if playlist contains the duplicated item
    while True:
        playlist = playlists_list_mine(client_instance,
                                       part='snippet,contentDetails',
                                       mine=True,
                                       maxResults=5,
                                       pageToken=next_page_token)

        for item in playlist['items']:
            if item['snippet']['title'] == key:
                return item['id']

        if 'nextPageToken' in playlist:
            # change page and keep searching
            next_page_token = playlist['nextPageToken']
        else:
            # end of the page, nothing has found
            return None


def add_video_to_playlist(client_instance, video_id, location):
    """
    add video to playlist

    Args:
        :client_instance
            resource => youtube resource
        :video_id
            string => the youtube video id
        :location
            string => title of the playlist

    Return:
        int => return the playlist id
    """
    playlist_id = None

    # get playlist and find if new one is duplicated
    playlist = playlists_list_mine(client_instance,
                                   part='snippet,contentDetails',
                                   mine=True,
                                   maxResults=5)

    # set true if target if found
    for item in playlist['items']:
        if item['snippet']['title'] == location:
            playlist_id = item['id']
            break

    # else find the following pages
    if playlist_id is None and 'nextPageToken' in playlist:
        playlist_id = search_target_playlist(client_instance,
                                             location, playlist['nextPageToken'])

    # insert new playlist if neccessary
    if playlist_id is None:
        playlist_id = playlists_insert(client_instance,
                                       {'snippet.title': location,
                                        'snippet.description': location,
                                        'snippet.tags[]': location,
                                        'snippet.defaultLanguage': '',
                                        'status.privacyStatus': 'public'},
                                       part='snippet,status')

    # add video to playlist
    is_item_uploaded = playlist_items_insert(client_instance,
                                             {'snippet.playlistId': playlist_id,
                                              'snippet.resourceId.kind': 'youtube#video',
                                              'snippet.resourceId.videoId': video_id,
                                              'snippet.position': ''},
                                             part='snippet')

    if is_item_uploaded:
        print('Video {} has been added to playlist {}'.format(
            video_id, playlist_id))
    else:
        print('Error: Failed to add video {} to playlist {}'.format(
            video_id, playlist_id))

    return playlist_id
