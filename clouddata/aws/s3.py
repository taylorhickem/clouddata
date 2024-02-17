import os
import boto3


s3_client = None


class S3Client(object):
    connected = False

    def __init__(self):
        pass

    def login(self):
        client_login()
        self.connected = True

    def client(self):
        return s3_client

    def __exit__(self):
        client_logout()

    def upload_file(self, bucket_name, filename, dir_local='', dir_remote='',
                    extra_args=None, callback=None, config=None
        ):
        if dir_remote:
            key = f'{dir_remote}/{filename}'
        else:
            key = filename
        if dir_local:
            filepath = os.path.join(dir_local, filename)
        else:
            filepath = filename
        self.client().upload_file(
            Filename=filepath,
            Bucket=bucket_name,
            Key=key,
            ExtraArgs=extra_args,
            Callback=callback,
            Config=config
        )


def client_login():
    global s3_client
    if not s3_client:
        s3_client = boto3.client('s3')

def client_logout():
    global s3_client
    if not s3_client:
        s3_client = None