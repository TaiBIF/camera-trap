# ==============================================
# Define system parameters from os.environ
# User can access the field by using .
# Example: SYS_PARAMS.SRC_BUCKET
# ==============================================

import os

class ObjDict(dict):
    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError("No such attribute: " + name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name in self:
            del self[name]
        else:
            raise AttributeError("No such attribute: " + name)

# define static system variable for access and management
SYS_PARAMS = ObjDict({
    'SRC_BUCKET': os.environ['SRC_BUCKET'],
    'YOUTUBE_VIDEO_URL': os.environ['YOUTUBE_VIDEO_URL'],
    'DIR': os.environ['DIR'],
    'ENDPOINT_MMA': os.environ['ENDPOINT_MMA'],
    'ENDPOINT_MMM': os.environ['ENDPOINT_MMM'],
    'CLIENT_ID': os.environ['CLIENT_ID'],
    'CLIENT_SECRET': os.environ['CLIENT_SECRET'],
    'REFRESH_TOKEN': os.environ['REFRESH_TOKEN'],
    'TAIBIF_API_URL': os.environ['TAIBIF_API_URL']
})
