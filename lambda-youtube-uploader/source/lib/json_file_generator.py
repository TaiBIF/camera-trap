# =======================================
# This class is for processing json file 
# =======================================

import datetime
import json
from lib.sys_params import SYS_PARAMS

from lib.s3_helpers import upload_json_file
from lib.common_helpers import generate_location_path
from lib.common_helpers import to_md5_hexdigest

class JsonFileGenerator:

    def __init__(self, **kwargs):
        self.bucket = kwargs.setdefault('bucket', '')
        self.youtube_url = kwargs.setdefault('youtube_url', '')
        self.youtube_playlist_id = kwargs.setdefault('youtube_playlist_id', '')
        self.project = kwargs.setdefault('project', 'NULL')
        self.site = kwargs.setdefault('site', 'NULL')
        self.sub_site = kwargs.setdefault('sub_site', 'NULL')
        self.location = kwargs.setdefault('location', 'NULL')
        self.video_name = kwargs.setdefault('video_name', '')
        self.video_length = kwargs.setdefault('video_length', '')
        self.video_org_datetime = kwargs.setdefault('video_org_datetime', datetime.datetime.now())
        self.video_mod_datetime = kwargs.setdefault('video_mod_datetime', datetime.datetime.now())
        self.video_width = kwargs.setdefault('video_width', '')
        self.video_height = kwargs.setdefault('video_height', '')
        self.user_id = kwargs.setdefault('user_id', 'NULL')
        self.upload_session_id = kwargs.setdefault('upload_session_id', '')
        self.device_metadata = kwargs.setdefault('device_metadata', {}) # 與相機相關但非 EXIF 的 Metadata 
        self.exif = kwargs.setdefault('exif', {}) # EXIF 整組 json 
        self.make = kwargs.setdefault('make', '') # 相機製造商
        self.model = kwargs.setdefault('model', '') # 相機型號
        self.full_location = generate_location_path(self.project, self.site, self.sub_site, self.location)

        self.enpoint_mma = SYS_PARAMS.ENDPOINT_MMA
        self.enpoint_mmm = SYS_PARAMS.ENDPOINT_MMM

    def do_process(self):
        """
        do the main process

        Args:
            None

        Return:
            None
        """

        file_types = ['mma', 'mmm']
        for json_type in file_types:
            self.process_json_file(json_type)

    def process_json_file(self, file_type):
        """
        create json file and upload to s3 bucket

        Args:
            file_type: 
                type of json, input mma or mmm

        Return:
            None
        """

        if file_type == 'mma':
            body = self.generate_mma_template()
            endpoint = self.enpoint_mma
        elif file_type == 'mmm':
            body = self.generate_mmm_template()
            endpoint = self.enpoint_mmm
        else:
            return None
        
        print(body)

        data = json.dumps({
            'endpoint': endpoint,
            'post': [body]
        }, ensure_ascii=False).encode('utf8')

        # create file key for uploading to s3 bucket
        file_key = 'json/{}/{}.{}.json'.format(self.upload_session_id, self.video_name, file_type)

        # upload json to s3 bucket
        upload_json_file(self.bucket, file_key, data)

    def generate_mma_template(self):
        """
        create mma json template

        Args:
            None

        Return:
            None
        """

        return {
            '_id': to_md5_hexdigest(self.youtube_url),
            'project': self.project,
            'full_location_md5': to_md5_hexdigest(self.full_location),
            '$set': {
                'modified_by': self.user_id,
                'type': 'MovingImage',
                'date_time_original': self.video_org_datetime.strftime("%Y-%m-%d %H:%M:%S"),  # 格式以 metadata 中擷取出來的為準
                'length_of_video': self.video_length,  # 暫時以 metadata 中擷取出來的為準
                'youtube_playlist_id': self.youtube_playlist_id
            },
            '$setOnInsert': {
                'url': self.youtube_url,
                'url_md5': to_md5_hexdigest(self.youtube_url), 
                'date_time_original_timestamp': int(self.video_org_datetime.timestamp()), # 由 date_time_original 轉換而來
                'date_time_corrected_timestamp': int(self.video_org_datetime.timestamp()), # 這邊此值等於 date_time_original_timestamp
                'project': self.project,
                'site': self.site,
                'sub_site': self.sub_site,
                'location': self.location,
                'full_location_md5': to_md5_hexdigest(self.full_location),
                'uploaded_file_name': self.video_name,
                'timezone': '+8',
                'tokens': [
                    {
                        'data': [
                            {
                                'key': 'species',
                                'label': '物種',
                                'value': ''
                            }
                        ]
                    }
                ]
            },
            '$addToSet': {
                'related_upload_sessions': self.upload_session_id
            },
            '$upsert': True
        }

    def generate_mmm_template(self):
        """
        create mmm json template

        Args:
            None

        Return:
            None
        """

        return {
            '_id': to_md5_hexdigest(self.youtube_url),
            'project': self.project,
            'full_location_md5': to_md5_hexdigest(self.full_location),
            '$set': {
                'modified_by': self.user_id,
                'type': 'MovingImage',
                'date_time_original': self.video_org_datetime.strftime("%Y-%m-%d %H:%M:%S"), # 格式以metadata 中擷取出來的為準
                'length_of_video': self.video_length, # 暫時以 metadata 中擷取出來的為準
                'youtube_playlist_id': self.youtube_playlist_id, 
                'device_metadata': self.device_metadata, # 與相機相關但非 EXIF 的 Metadata 整組直接以 json 先塞在這
                'exif': self.exif, # EXIF 整組先以 json 塞在這
                'make': self.make, # 相機製造商(如果有此項資訊的話)
                'model': self.model, # 相機型號(如果有此項資訊的話)
                'modify_date': self.video_mod_datetime.strftime("%Y-%m-%d %H:%M:%S"), # device_metadata 中的檔案編修時間
                'width': self.video_width,
                'height': self.video_height
            },
            '$setOnInsert': {
                'url': self.youtube_url,
                'url_md5': to_md5_hexdigest(self.youtube_url),
                'date_time_original_timestamp': int(self.video_org_datetime.timestamp()), # 由date_time_original 轉換而來
                'date_time_corrected_timestamp': int(self.video_org_datetime.timestamp()), # 這邊此值等於 date_time_original_timestamp
                'project': self.project,
                'site': self.site,
                'sub_site': self.sub_site,
                'location': self.location,
                'full_location_md5': to_md5_hexdigest(self.full_location),
                'uploaded_file_name': self.video_name,
                'timezone': '+8'
            },
            '$upsert': True
        }
