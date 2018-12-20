# ========================
# Extract video metadata
# ========================

from hachoir.parser import createParser
from hachoir.metadata import extractMetadata
from sys import stderr
import os
from lib.sys_params import SYS_PARAMS
import datetime
import pytz

def extra_video_meta(file_name):
    """
    get necessary information from video

    Args:
        file_name: 
            string => the target file name

    Return:
        dict => result
    """

    parser = createParser(file_name)
    if not parser:
        print("Unable to parse file", file=stderr)
        return {}

    with parser:
        try:
            metadata = extractMetadata(parser)
            print(metadata)
        except Exception as err:
            print("Metadata extraction error: %s" % err)
            metadata = None

    if not metadata:
        print("Unable to extract metadata")
        return {}

    # init variables
    statinfo = os.stat(file_name)

    # create time and modification time from file info
    # THESE OPERATIONS ARE TOTALLY USELESS DUE TO MAKING COPIES TO TMP AT THE BEGINNING OF THIS LAMBDA FUNCTION
    # THE file_ctime AND file_mtime ARE ALWAYS SET TO THE TIME MAKING THOSE COPIES
    # TODO: FIND RELATIVELY SOLID ctime AND mtime FROM METADATA
    file_ctime = datetime.datetime.fromtimestamp(statinfo.st_ctime).astimezone(pytz.timezone('Asia/Taipei'))
    file_mtime = datetime.datetime.fromtimestamp(statinfo.st_mtime).astimezone(pytz.timezone('Asia/Taipei'))
    print("File created at %d, %s" % (statinfo.st_ctime, file_ctime))
    print("File modified at %d, %s" % (statinfo.st_mtime, file_mtime))

    # information from metadata
    date_time_original = metadata._Metadata__data['date_time_original']
    date_creation = metadata._Metadata__data['creation_date']
    last_modification = metadata._Metadata__data['last_modification']
    duration = get_metadata_default_value(metadata._Metadata__data, 'duration', 'text') 
    make = get_metadata_default_value(metadata._Metadata__data, 'camera_manufacturer', 'value') 
    model = get_metadata_default_value(metadata._Metadata__data, 'camera_model', 'value')
    res_width = get_metadata_default_value(metadata._Metadata__data, 'width', 'value')
    res_height = get_metadata_default_value(metadata._Metadata__data, 'height', 'value') 

    # leave empty for now
    device_metadata = {}
    exif = {}

    # if date original does not exist, check date_creation
    # if still gets no value, give empty string 
    if len(date_time_original.values) > 0:
        date_time_original = date_time_original.values[0].value
    else:
        if len(date_creation.values) > 0:
            date_time_original = date_creation.values[0].value
        else:
            date_time_original = file_ctime
    
    # check if value exists, else give empty string
    last_modification = last_modification.values[0].value if len(last_modification.values) > 0 else file_mtime

    return {
        'duration': duration,
        'date_time_original': date_time_original,
        'date_last_modification': last_modification,
        'device_metadata': device_metadata,
        'exif': exif, 
        'make': make,
        'model': model,
        'height': res_height,
        'width': res_width
    }

def get_metadata_default_value(target_dict, selector, key):
    """
    get attribute and set default value if it has empty value

    Args:
        file_name: 
            string => the target file name

    Return:
        string => value from the key
    """

    tmp_obj = target_dict[selector]
    return getattr(tmp_obj.values[0], key) if len(tmp_obj.values) > 0 else ''