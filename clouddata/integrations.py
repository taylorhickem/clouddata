from .aws.s3 import S3Client
from . import gdrive


class GDriveToS3(object):
    s3client = None
    gdclient = None
    connected = False

    def __init__(self):
        self.s3client = S3Client()
        self.gdclient = gdrive.GDriveClient()

    def login(self):
        self.s3client.login()
        self.gdclient.login()
        self.connected = True

    def __repr__(self):
        return f'<{self.__class__} Integration object>'
