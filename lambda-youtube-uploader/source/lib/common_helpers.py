# ========================
# Helpers for commen usage
# ========================

from lib.sys_params import SYS_PARAMS
import hashlib

def get_full_download_path(file_name):
    """
    get download path of the temporary directory

    Args:
        :file_name
            string => target file

    Return:
        string => path of the temporary directory
    """

    return '{}{}'.format(SYS_PARAMS.DIR, file_name)

def generate_location_path(projectId, site, subSite, cameraLocation):
    """
    create a path by giving parameters, this is for the cameraLocation of TaiBIF 

    Args:
        :projectId => string
        :site => string
        :subSite => string
        :cameraLocation => string

    Return:
        string => cameraLocation for TaiBIF
    """
    
    return '{}/{}/{}/{}'.format(projectId, site, subSite, cameraLocation)

def to_md5_hexdigest(input_string):
    """
    convert string to hexdigest

    Args:
        :input_string
            string => target string

    Return:
        string => string after md5
    """

    return hashlib.md5(input_string.encode('utf8')).hexdigest()