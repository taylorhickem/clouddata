import os
import json
from .aws.s3 import S3Client
from .gdrive import GDriveClient
from . import archive


GDRIVE_SERVICE = 'google_drive'


class GDriveToS3(object):
    s3client = None
    gdclient = None
    connected = False

    def __init__(self):
        self.s3client = S3Client()
        self.gdclient = GDriveClient()

    def login(self):
        self.s3client.login()
        self.gdclient.login()
        self.connected = True

    def directory_archive_to_s3(self, gdrive_folder,
        s3_bucket, gdrive_folder_id='', google_account_id='', archive_tag='', bucket_prefix='',
        path_local='', security_class='', permissions='', expiration_date='',
        status_updates=False):
        folder_name_no_spaces = gdrive_folder.replace(' ', '_')
        archive_name = f'{archive_tag}_{folder_name_no_spaces}'
        s3_dir_path = f's3://{s3_bucket}/{bucket_prefix}/'
        archive_success = False
        zip_filename = archive.DATA_FILE_DEFAULT
        dir_config_filename = archive.DIRECTORY_FILE_DEFAULT
        zip_filepath = os.path.join(path_local, zip_filename)
        config_filepath = os.path.join(path_local, dir_config_filename)
        errors = ''
        #01 get directory info from gdrive
        step_success = False
        dir_config_path = ''
        if status_updates:
            print(f'INFO. getting directory information for google drive folder {gdrive_folder} ...')
        try:
            dir_config_path = self.gdclient.get_directory_config(
                gdrive_folder, folder_id=gdrive_folder_id, save_to_file=True,
                filepath=config_filepath
            )
            step_success = len(dir_config_path) > 0
        except Exception as e:
            step_success = False
            errors = f'ERROR. failed to get directory information for drive folder {gdrive_folder}. {str(e)}'
        if status_updates:
            if step_success:
                print(f'INFO. directory information saved to {dir_config_path}.')
            else:
                print(errors)

        #02 download files from gdrive and zip
        config = {}
        if step_success:
            if status_updates:
                print(f'INFO. downloading files from Google Drive to zip ...')
            with open(dir_config_path, 'r') as f:
                config = json.load(f)
                f.close()
            try:
                step_success, errors = self.gdclient.directory_download(
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

        #03 create archive
        if step_success:
            if status_updates:
                print(f'INFO. creating archive: {archive_name} ...')
            try:
                archive_obj = archive.DirectoryArchive(
                    data_file=zip_filepath,
                    directory_file=dir_config_path,
                    name=archive_name,
                    path_local=path_local,
                    service_provider=GDRIVE_SERVICE,
                    account_id=google_account_id,
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

        #04 upload to s3
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

        archive_success = step_success
        return archive_success, errors

    def __repr__(self):
        return f'<{self.__class__} Integration object>'
