# =======================
# Module for TaiBIF APIs
# =======================

from lib.sys_params import SYS_PARAMS
import json
from http import HTTPStatus
from urllib.error import HTTPError

import requests

REQ_HEADER = {
    'Content-Type': 'application/json'
}

TAIBIF_API_URL = SYS_PARAMS.TAIBIF_API_URL

def create_endpoint(resource, action):
    """
    create an endpoint by giving resourcr and action names

    Args:
        resource: 
            string => name of the resource
        action: 
            string => name of the action
    Return:
        string => endpoint url
    """

    return '{}/{}/{}'.format(TAIBIF_API_URL, resource, action)

def query_multimedia_metadata(file_name, original_datetime, full_location):
    """
    check if metadata exists in TaiBIF

    Args:
        file_name: 
            string => target file name
        original_datetime: 
            string => video original datetime in timestamp
        full_location: 
            string => full location after md5

    Return:
        object => result
    """

    endpoint = create_endpoint('media', 'query')

    payload = json.dumps({
        'query':{
            'uploaded_file_name': file_name,
            'date_time_original_timestamp': original_datetime,
            'fullCameraLocationMd5': full_location
        } 
    }, ensure_ascii=False).encode('utf8')

    try:
        resp = requests.post(endpoint, headers=REQ_HEADER, data=payload)
        json_data = json.loads(resp.content.decode())

        if resp.status_code == HTTPStatus.OK:
            print('taibif api - metadata: {}'.format(json_data))
        else:
            print('status code: {}, reason: {}. full messag: {}'.format(resp.status_code, resp.reason, resp.text))

    except HTTPError as e:
        print(e)
        raise

    return json_data


