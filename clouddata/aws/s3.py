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


def client_login():
    global s3_client
    if not s3_client:
        s3_client = boto3.client('s3')

def client_logout():
    global s3_client
    if not s3_client:
        s3_client = None