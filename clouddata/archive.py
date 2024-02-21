import os
import shutil
import json
import re
import datetime as dt
from .aws.dynamodb import DynamoDBAPI
from .aws.s3 import S3Client
from .gdrive import GDriveClient


DATA_FILE_DEFAULT = 'data.zip'
DIRECTORY_FILE_DEFAULT = 'directory.json'
DIR_MAP_FILENAME_TEMPLATE = '{archive_id}.json'
METADATA_FILE = 'metadata.json'
ARCHIVE_ID_DEFAULT = 'archive_{timestamp}'
ARCHIVE_ID_NAMED = 'archive_{timestamp}_{name}'
TIMESTAMP_FORMAT_ARCHIVE_ID = '%Y%m%d%H%M'
TIMESTAMP_FORMAT_STD = '%Y-%m-%d %H:%M'
DATE_FORMAT = '%Y-%m-%d'
METADATA_DEFAULT = {
    'id': '',
    'name': '',
    'timestamp': '',
    'service_provider': '',
    'account_id': '',
    'directory_path': '',
    'directory_url': '',
    'size_mb': '',
    'directory_file': DIRECTORY_FILE_DEFAULT,
    'data_file': DATA_FILE_DEFAULT,
    'security_class': '',
    'permissions': '',
    'expiration_date': ''
}
GDRIVE_SERVICE = 'google_drive'
ARCHIVE_TABLENAME = 'data_archive'
ARCHIVE_DB_CONFIG_FILE = 'data_archive_db_config.json'
ARCHIVE_RECORD_DEFAULT = {
    'id': '',
    'timestamp': '',
    'bucket': '',
    'location_path': '',
    'dir_map_bucket': '',
    'dir_map_prefix': '',
    'data_key': '',
    'tags': [],
    'metadata': {}
}


class DirectoryArchive(object):
    id = ''
    name = ''
    timestamp = ''
    metadata = METADATA_DEFAULT.copy()
    directory = {}
    data_file = ''
    directory_file = ''
    path_local = ''
    path_remote = ''
    path_url = ''

    def __init__(self,
                 data_file=DATA_FILE_DEFAULT,
                 directory_file=DIRECTORY_FILE_DEFAULT,
                 timestamp='', name='', path_local='', path_remote='', path_url='', **kwargs):
        args = locals().copy()
        for k in args:
            setattr(self, k, args[k])
        if not self.timestamp:
            self.timestamp = dt.datetime.now().strftime(TIMESTAMP_FORMAT_STD)
        self._set_id()
        self._metadata_update(**kwargs)

    def _set_id(self):
        id_timestamp = dt.datetime.strptime(
            self.timestamp, TIMESTAMP_FORMAT_STD
        ).strftime(TIMESTAMP_FORMAT_ARCHIVE_ID)
        if self.name:
            self.id = ARCHIVE_ID_NAMED.format(
                name=self.name,
                timestamp=id_timestamp
            )
        else:
            self.id = ARCHIVE_ID_DEFAULT.format(
                timestamp=id_timestamp
            )

    def _metadata_update(self, **kwargs):
        for k in self.metadata:
            if k in self.__dict__:
                self.metadata[k] = self.__dict__[k]
            if k in kwargs:
                self.metadata[k] = kwargs[k]

    def _parse_directory(self):
        if not self.directory:
            if os.path.exists(self.directory_file):
                with open(self.directory_file, 'r') as f:
                    self.directory = json.load(f)
                    f.close()
        if self.directory:
            self.size_mb = self.directory['size_mb']
        self._metadata_update()

    def save(self):
        """save archive to local files to path_local/archive_id
        """
        self._parse_directory()
        archive_dir = os.path.join(self.path_local, self.id)
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)

        metadata = self.metadata.copy()
        metadata['directory_file'] = DIRECTORY_FILE_DEFAULT
        metadata['data_file'] = DATA_FILE_DEFAULT
        with open(METADATA_FILE, 'w') as f:
            json.dump(metadata, f, indent=2)
            f.close()
        os.rename(METADATA_FILE, os.path.join(archive_dir, METADATA_FILE))
        os.rename(self.directory_file, os.path.join(archive_dir, metadata['directory_file']))
        os.rename(self.data_file, os.path.join(archive_dir, metadata['data_file']))
        return archive_dir

    def __repr__(self):
        return self.id


class ArchiveDB(object):
    api = None
    table = None
    table_name = ARCHIVE_TABLENAME
    db_config = {}
    dir_maps_bucket = ''
    dir_maps_prefix = ''
    is_connected = False

    def __init__(self, dir_maps_bucket, dir_maps_prefix='', config_path=''):
        for k in ['bucket', 'prefix']:
            self.__dict__[f'dir_maps_{k}'] = locals()[f'dir_maps_{k}']
        self._config_load(config_path=config_path)
        if self.db_config:
            self.api = DynamoDBAPI(config=self.db_config)

    def _config_load(self, config_path=''):
        if not config_path:
            config_path = os.path.join(os.path.dirname(__file__), ARCHIVE_DB_CONFIG_FILE)
        with open(config_path, 'r') as f:
            config = json.load(f)
            f.close()
        self.db_config = config

    def connect(self):
        if self.api:
            self.api.connect()
            self.is_connected = self.api.is_connected()
            self.table = self.api._get_table_obj(ARCHIVE_TABLENAME)

    def disconnect(self):
        if self.table:
            self.table.unload()
            self.table = None
        if self.api:
            self.api.disconnect()
            self.api = None
        self.is_connected = False

    def __exit__(self):
        self.disconnect()

    def record_get(self, archive_id) -> dict:
        item = self._item_get(archive_id)
        record = self._as_record(item)
        return record

    def record_put(self, record):
        item = self._as_item(record)
        self._item_put(item)

    def _item_get(self, archive_id) -> dict:
        item = {}
        keys, items = self.table.query_by_key_str_value_eq(
            'id', archive_id)
        if len(items) > 0:
            item = items[0]
        return item

    def _item_put(self, item):
        self.table.table.put_item(Item=item)

    def _as_item(self, record) -> dict:
        item = record
        return item

    def _as_record(self, item) -> dict:
        record = item
        return record

    def record_from_metadata(self, metadata) -> dict:
        record = ARCHIVE_RECORD_DEFAULT.copy()
        for f in [
            'id',
            'timestamp'
        ]:
            record[f] = metadata[f]
        for k in ['bucket', 'prefix']:
            record[f'dir_maps_{k}'] = self.__dict__[f'dir_maps_{k}']
        archive_id = metadata['id']
        bucket_regex = r'^s3:\/\/([^\/]+)/'
        if metadata.get('directory_path', ''):
            directory_path = metadata['directory_path']
            match = re.match(bucket_regex, directory_path)
            if match:
                bucket_name = match.group(1)
                record['bucket'] = bucket_name
                if metadata.get('data_file', ''):
                    data_file = metadata.get('data_file', '')
                    location_path = directory_path.replace(f's3://{bucket_name}/', '')
                    location_path = f'{location_path}{archive_id}/'
                    record['location_path'] = location_path
                    record['data_key'] = f'{location_path}{data_file}'

        record['metadata'] = metadata
        return record


class S3ArchiveClient(object):
    s3client = None
    data_client = None
    data_service = ''
    connected = False

    def __init__(self, data_service=''):
        self.s3client = S3Client()
        if data_service:
            self.data_service = data_service
            self.data_client = get_data_client(data_service)

    def login(self):
        self.s3client.login()
        if self.data_client:
            self.data_client.login()
        self.connected = True

    def directory_archive_to_s3(self, **kwargs):
        if self.data_service == 'google_drive':
            self.gdrive_folder_archive_to_s3(**kwargs)

    def gdrive_folder_archive_to_s3(self, gdrive_folder,
            s3_bucket, gdrive_folder_id='', google_account_id='', archive_tag='', bucket_prefix='',
            dir_map_bucket='', dir_map_prefix='', path_local='', security_class='',
            permissions='', expiration_date='', status_updates=False, cleanup=True):

        service_provider = GDRIVE_SERVICE
        account_id = google_account_id
        folder_name_no_spaces = gdrive_folder.replace(' ', '_')
        archive_name = f'{archive_tag}_{folder_name_no_spaces}'
        s3_dir_path = f's3://{s3_bucket}/{bucket_prefix}/'
        zip_filename = DATA_FILE_DEFAULT
        dir_config_filename = DIRECTORY_FILE_DEFAULT
        zip_filepath = os.path.join(path_local, zip_filename)
        config_filepath = os.path.join(path_local, dir_config_filename)

        step_success = False
        errors = ''

        #01 get directory info from gdrive
        dir_config_path = ''
        if status_updates:
            print(f'INFO. getting directory information for google drive folder {gdrive_folder} ...')
        try:
            dir_config_path = self.data_client.get_directory_config(
                gdrive_folder, folder_id=gdrive_folder_id, save_to_file=True,
                filepath=config_filepath
            )
            step_success = len(dir_config_path) > 0
        except Exception as e:
            errors = f'ERROR. failed to get directory information for drive folder {gdrive_folder}. {str(e)}'
        if status_updates:
            if step_success:
                print(f'INFO. directory information saved to {dir_config_path}.')
            else:
                print(errors)

        #02 download files from gdrive and zip
        if step_success:
            if status_updates:
                print(f'INFO. downloading files from Google Drive to zip ...')
            with open(dir_config_path, 'r') as f:
                config = json.load(f)
                f.close()
            try:
                step_success, errors = self.data_client.directory_download(
                    gdrive_folder, folder_id=gdrive_folder_id,
                    dir_path=path_local, zip_filename=zip_filepath,
                    as_zip=True, config=config, print_updates=status_updates)
            except Exception as e:
                step_success = False
                errors = f'ERROR. failed to download directory {gdrive_folder}. {str(e)}'
        if status_updates:
            if step_success:
                print(f'INFO. directory {gdrive_folder} downloaded to {zip_filepath}.')
            else:
                print(errors)

        archive_path = ''
        #03 create local archive
        if step_success:
            if status_updates:
                print(f'INFO. creating archive: {archive_name} ...')
            try:
                archive_obj = DirectoryArchive(
                    data_file=zip_filepath,
                    directory_file=dir_config_path,
                    name=archive_name,
                    path_local=path_local,
                    service_provider=service_provider,
                    account_id=account_id,
                    directory_path=s3_dir_path,
                    security_class=security_class,
                    permissions=permissions,
                    expiration_date=expiration_date
                )
                archive_obj.save()
                archive_id = archive_obj.id
                archive_path = os.path.join(path_local, archive_id)
                metadata = archive_obj.metadata.copy()
                size_mb = metadata['size_mb']
                step_success = True
            except Exception as e:
                step_success = False
                errors = f'ERROR. Failed to save archive {archive_name}. {str(e)}'
        if status_updates:
            if step_success:
                print(f'INFO. archive created at {archive_obj.timestamp} with id {archive_id} and size {size_mb} MB.')
            else:
                print(errors)

        #03 create DB record
        if step_success:
            if status_updates:
                print(f'INFO. creating archive DB record for archive_id {archive_id} ...')
            try:
                db_client = ArchiveDB(
                    dir_maps_bucket=dir_map_bucket,
                    dir_maps_prefix=dir_map_prefix
                )
                db_client.connect()
                db_record = db_client.record_from_metadata(archive_obj.metadata)
                db_client.record_put(db_record)
                db_client.disconnect()
            except Exception as e:
                step_success = False
                errors = f'ERROR. Failed to create archive record from metadata. {str(e)}'

        if status_updates:
            if step_success:
                print(f'INFO. archive DB record created.')
            else:
                print(errors)

        #04 upload directory map to S3
        if step_success:
            dir_map_filename = DIR_MAP_FILENAME_TEMPLATE.format(
                archive_id=archive_id
            )
            dir_map_tmp_path = os.path.join(path_local, dir_map_filename)
            shutil.copyfile(
                os.path.join(archive_path, DIRECTORY_FILE_DEFAULT),
                dir_map_tmp_path)
            if status_updates:
                print(f'INFO. uploading directory map file {dir_map_filename} for archive_id {archive_id} to S3 bucket {dir_map_bucket} location {dir_map_prefix} ...')
            try:
                self.s3client.upload_file(
                    dir_map_bucket,
                    dir_map_filename,
                    dir_local=path_local,
                    dir_remote=dir_map_prefix
                )
                os.remove(dir_map_tmp_path)
            except Exception as e:
                step_success = False
                errors = f'ERROR. failed to upload file {dir_map_filename}. {str(e)}'

        if status_updates:
            if step_success:
                print(f'INFO. directory map uploaded to S3.')
            else:
                print(errors)

        #05 upload archive data to s3
        if step_success:
            s3_archive_prefix = f'{bucket_prefix}/{archive_id}'
            if status_updates:
                print(f'INFO. uploading archive to s3 bucket {s3_bucket} location {s3_archive_prefix} from local {archive_path} ...')
            for root, dirs, files in os.walk(archive_path):
                for file in files:
                    if step_success:
                        if status_updates:
                            print(f'INFO. uploading archive file {file} to s3 bucket prefix {s3_archive_prefix}...')
                        try:
                            self.s3client.upload_file(
                                s3_bucket,
                                file,
                                dir_local=archive_path,
                                dir_remote=s3_archive_prefix
                            )
                        except Exception as e:
                            step_success = False
                            errors = f'ERROR. failed to upload {file}. {str(e)}'

        if status_updates:
            if step_success:
                print(f'INFO. archive upload complete.')
            else:
                print(errors)

        #06 local archive directory cleanup
        if cleanup:
            if status_updates:
                print(f'INFO. cleaning up local archive files in {archive_path} ...')
            if os.path.exists(archive_path):
                shutil.rmtree(archive_path, ignore_errors=True)

        if status_updates:
            if step_success:
                print(f'INFO. local archive files removed.')
            else:
                print(errors)

        archive_success = step_success
        return archive_success, errors

    def __repr__(self):
        return f'<{self.__class__} Integration object>'


def get_data_client(data_service, **config):
    data_client = None
    if data_service == 'google_drive':
        data_client = GDriveClient(**config)
    return data_client
