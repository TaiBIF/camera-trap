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

def generate_location_path(project, site, sub_site, location):
    """
    create a path by giving parameters, this is for the location of TaiBIF 

    Args:
        :project => string
        :site => string
        :sub_site => string
        :location => string

    Return:
        string => location for TaiBIF
    """
    
    return '{}/{}/{}/{}'.format(project, site, sub_site, location)

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