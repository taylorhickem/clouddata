""" this module uses the Google Drive API to
 perform file management operations on a Google Drive account
 for details refer to Developer documentation
 https://developers.google.com/drive/api/guides/about-files
"""
#-----------------------------------------------------------------------------
# import dependencies
#-----------------------------------------------------------------------------
import io
import shutil
import zipfile
import os
import json
import datetime as dt
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


#-----------------------------------------------------------------------------
# module variables
#-----------------------------------------------------------------------------
SCOPES = ['https://www.googleapis.com/auth/drive']
CLIENT_SECRET_DEFAULT = 'client_secret.json'
ordRef = {'A': 65}
BYTES_PER_MB = 1000000
JSON_INDENT = 4
FIELDS_DEFAULT = 'nextPageToken, files(kind, {mime_type}, id, name, parents, {last_modified}, {size_bytes})'
MODIFIED_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
CONFIG_DIR = ''
DIRECTORY_CONFIG_FILENAME = 'directory.json'
CONFIG_FILES = {
    'QUERY_OPERATORS': {
        'file_type': 'json',
        'filename': 'google_api_query_operators.json'
    },
    'API_PARAMETERS': {
        'file_type': 'json',
        'filename': 'google_api_parameters.json'
    }
}
DIR_CONFIG_DEFAULT = {
    'name': '',
    'last_modified': '',
    'size_mb': '',
    'size_bytes': '',
    'folders': [],
    'native_files': [],
    'non_native_files': []
}
BUILT_IN_QUERIES = [
    'folder_contents',
    'files_in_folder',
    'native_files_in_folder',
    'non_native_files_in_folder',
    'folders_in_folder',
    'folders_only',
    'files_only'
]
QUERY_OPERATORS = {}
API_PARAMETERS = {}
DRIVE_PARAMETERS = {}


#-----------------------------------------------------------------------------
# config data
#-----------------------------------------------------------------------------

def config_load():
    global CONFIG_DIR
    CONFIG_DIR = os.path.dirname(__file__)
    for cf in CONFIG_FILES:
        filename = CONFIG_FILES[cf]['filename']
        file_type = CONFIG_FILES[cf]['file_type']
        filepath = os.path.join(CONFIG_DIR, filename)
        with open(filepath, 'r') as f:
            if file_type == 'json':
                data = json.load(f)
            elif file_type == 'text':
                data = f.read()
            globals()[cf] = data
            f.close()
    _set_api_parameters()


def _set_api_parameters():
    global FIELDS_DEFAULT, DRIVE_PARAMETERS
    DRIVE_PARAMETERS = [api['parameters'] for api in API_PARAMETERS['apis'] if api['api'] == 'drive'][0]
    FIELDS_DEFAULT = FIELDS_DEFAULT.format(**DRIVE_PARAMETERS)


#-----------------------------------------------------------------------------
# GDriveClient class
#-----------------------------------------------------------------------------


class GDriveClient(object):
    client_secret_path = ''
    service = None
    loaded = False

    def __init__(self, client_secret_path=''):
        config_load()
        self._set_secret_file_path(client_secret_path=client_secret_path)

    def login(self):
        credentials = self._get_credentials()
        http = credentials.authorize(Http())
        self.service = build('drive', 'v3', http=http, cache_discovery=False)

    def _set_secret_file_path(self, client_secret_path=''):
        if not self.loaded:
            self.client_secret_path = client_secret_path if client_secret_path else CLIENT_SECRET_DEFAULT
            self.loaded = True

    def _get_credentials(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.client_secret_path,
            SCOPES
        )
        return credentials

    def query_contents(self, qry_stm='', query_alias='', fields='', **args):
        if not fields:
            fields = FIELDS_DEFAULT
        if not qry_stm:
            qry_stm = self._get_query_stm(query_alias, **args)
        response = self.service.files().list(
            q=qry_stm,
            spaces='drive',
            fields=fields
        ).execute()
        contents = response['files']
        return contents

    @staticmethod
    def _get_query_stm(query_alias, **args):
        if query_alias in BUILT_IN_QUERIES:
            mime_type_field = DRIVE_PARAMETERS['mime_type']
            qry = ''
            if query_alias == 'folder_contents':
                qry = "'" + args['folder_id'] + "' in parents"
            elif query_alias == 'files_in_folder':
                qry = "'" + args['folder_id'] + "' in parents"
                qry = qry + " and " + QUERY_OPERATORS['isnota_folder']
            elif query_alias == 'native_files_in_folder':
                qry = "'" + args['folder_id'] + "' in parents"
                qry = qry + " and " + QUERY_OPERATORS['isa_native']
            elif query_alias == 'non_native_files_in_folder':
                qry = "'" + args['folder_id'] + "' in parents"
                qry = qry + " and " + QUERY_OPERATORS['isnota_native_file']
            elif query_alias == 'folders_in_folder':
                qry = "'" + args['folder_id'] + "' in parents"
                qry = qry + " and " + QUERY_OPERATORS['isa_folder']
            elif query_alias == 'folders_only':
                qry = QUERY_OPERATORS['isa_folder']
            elif query_alias == 'files_only':
                qry = QUERY_OPERATORS['isnota_folder']
            if 'mime_type' in args:
                qry = qry + f" and {mime_type_field}='" + args['mime_type'] + "'"
        else:
            raise ValueError(f'invalid query_alias {query_alias}. Available aliases {BUILT_IN_QUERIES}')
        return qry

    def get_folder_id(self, folder_name):
        folder_id = ''
        qry_stm = "name='" + folder_name + "'"
        qry_stm = qry_stm + " and " + self._get_query_stm('folders_only')
        contents = self.query_contents(qry_stm=qry_stm)
        found_folder = len(contents) > 0
        if found_folder:
            folder_id = contents[0]['id']
        return folder_id

    def get_files_in_folder(self, folder_name='', folder_id='',
                            include_subfolders=False,
                            mime_type=None):
        contents = []
        args = {}
        if folder_id == '':
            folder_id = self.get_folder_id(folder_name)
        if len(folder_id) > 0:
            args['folder_id'] = folder_id
            if include_subfolders:
                query_alias = 'folder_contents'
            else:
                query_alias = 'files_in_folder'
            if mime_type:
                args['mime_type'] = mime_type
            contents = self.query_contents(query_alias=query_alias, **args)
        return contents

    def get_native_files_in_folder(self, folder_name='', folder_id=''):
        query_alias = 'native_files_in_folder'
        contents = []
        args = {}
        if folder_id == '':
            folder_id = self.get_folder_id(folder_name)
        if len(folder_id) > 0:
            args['folder_id'] = folder_id
            contents = self.query_contents(query_alias=query_alias, **args)
        return contents

    def get_non_native_files_in_folder(self, folder_name='', folder_id=''):
        query_alias = 'non_native_files_in_folder'
        contents = []
        args = {}
        if folder_id == '':
            folder_id = self.get_folder_id(folder_name)
        if len(folder_id) > 0:
            args['folder_id'] = folder_id
            contents = self.query_contents(query_alias=query_alias, **args)
        return contents

    def get_subfolders(self, folder_name='', folder_id='',
                            mime_type=None):
        contents = []
        args = {}
        if folder_id == '':
            folder_id = self.get_folder_id(folder_name)
        if folder_id:
            args['folder_id'] = folder_id
            query_alias = 'folders_in_folder'
            if mime_type:
                args['mime_type'] = mime_type
            contents = self.query_contents(query_alias=query_alias, **args)
        return contents

    def download_files_in_folder_as_bytes(self, folder_name, folder_id, mime_type=None):
        payloads = []
        names = []
        file_references = self.get_files_in_folder(
            folder_name, folder_id,
            include_subfolders=False,
            mime_type=mime_type)
        if len(file_references) > 0:
            for f in file_references:
                payload, errors = self.file_download(f['id'], f['name'], return_bytes=True)
                names.append(f['name'])
                if not errors:
                    payloads.append(payload)
                else:
                    payloads.append(errors)

        files = dict(zip(names, payloads))
        return files

    def move_file_to_folder(self, file_id,
                            destination_id, source_id=''):
        if source_id != '':
            response = self.service.files().update(
            fileId=file_id,
            addParents=destination_id,
            removeParents=source_id,
            fields='id, parents'
            ).execute()
        else:
            response = self.service.files().update(
            fileId=file_id,
            addParents=destination_id,
            fields='id, parents'
            ).execute()

    def move_files_to_folder(self, file_ids, destination_id, source_id=''):
        for f in file_ids:
            self.move_file_to_folder(f, destination_id, source_id)

    def get_file_parent_folder_ids(self, file_id):
        parent_ids = []
        response = self.service.files().get(
            fileId=file_id,
            fields='parents'
        ).execute()
        if 'parents' in response:
            parent_ids = response['parents']
        return parent_ids

    def get_directory_config(self, folder_name, folder_id='', save_to_file=False, filepath=''):
        config = DIR_CONFIG_DEFAULT.copy()
        config['name'] = folder_name

        for f in [
            'last_modified',
            'size_bytes'
        ]:
            del config[f]
            config[DRIVE_PARAMETERS[f]] = ''

        if folder_id == '':
            folder_id = self.get_folder_id(folder_name)
            config['id'] = folder_id

        subfolders = self.get_subfolders(folder_id=folder_id)
        native_files = self.get_native_files_in_folder(folder_id=folder_id)
        non_native_files = self.get_non_native_files_in_folder(folder_id=folder_id)

        config['native_files'] = native_files
        config['non_native_files'] = non_native_files

        sub_configs = []
        if subfolders:
            for s in subfolders:
                subfolder_id = s['id']
                subfolder_name = s['name']
                sub_config = self.get_directory_config(subfolder_name, folder_id=subfolder_id)
                sub_configs.append(sub_config)
            config['folders'] = sub_configs

        items = [native_files, non_native_files, sub_configs]
        max_date = get_max_date_from_items(items, date_key=DRIVE_PARAMETERS['last_modified'])
        if max_date:
            config[DRIVE_PARAMETERS['last_modified']] = dt.datetime.strftime(max_date, MODIFIED_DATE_FORMAT)

        size_bytes = get_size_bytes_from_items(items, size_key=DRIVE_PARAMETERS['size_bytes'])
        if size_bytes:
            config[DRIVE_PARAMETERS['size_bytes']] = str(size_bytes)
            config['size_mb'] = str(size_bytes/BYTES_PER_MB)

        if save_to_file:
            if not filepath:
                filepath = DIRECTORY_CONFIG_FILENAME
            write_to_file(config, filepath, 'json')
            return filepath
        else:
            return config

    def file_download(self, file_id, filename, dir_path='', return_bytes=False):
        download_success = False
        file = io.BytesIO()
        errors = ''

        try:
            request = self.service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(file, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()

        except HttpError as error:
            errors = f'ERROR. error when downloading file: {filename}. {error}'
            file = None

        if file:
            if return_bytes:
                return file.getvalue()
            else:
                filepath = os.path.join(dir_path, filename)
                try:
                    if dir_path:
                        if not os.path.exists(dir_path):
                            os.makedirs(dir_path)
                    with open(filepath, 'wb') as f:
                        f.write(file.getvalue())
                        file.close()
                        f.close()
                    download_success = True
                except Exception as e:
                    errors = f'ERROR. IO error writing bytes to file {filepath}. {str(e)}'
                return download_success, errors
        else:
            return None, errors

    def directory_download(self, folder_name, folder_id='', dir_path='',
                           include_native=False, include_non_native=True,
                           zip_filename='', as_zip=True,
                           config={},
                           print_updates=False):
        if print_updates:
            print(f'downloading contents from folder {folder_name} ...')
        errors = ''
        download_success = True
        contents_path = os.path.join(dir_path, folder_name)
        if not config:
            config = self.get_directory_config(
                folder_name,
                folder_id=folder_id
            )
        if not os.path.exists(contents_path):
            os.makedirs(contents_path)

        if config['folders']:
            for sub_config in config['folders']:
                if download_success:
                    try:
                        self.directory_download(
                            folder_name=sub_config['name'],
                            dir_path=contents_path,
                            include_native=include_native,
                            include_non_native=include_non_native,
                            as_zip=False,
                            config=sub_config
                        )
                    except Exception as e:
                        download_success = False
                        errors = str(e)

        if include_native and config['native_files']:
            native_file_names = [f['name'] for f in config['native_files']]
            print(f'WARNING: file download for native Google files not supported. files excluded: {native_file_names}')

        if include_non_native and config['non_native_files']:
            for file in config['non_native_files']:
                if download_success:
                    try:
                        self.file_download(
                            file_id=file['id'],
                            filename=file['name'],
                            dir_path=contents_path,
                            return_bytes=False
                        )
                    except Exception as e:
                        download_success = False
                        errors = str(e)

        if download_success and as_zip:
            if not zip_filename:
                zip_filename = f'{contents_path}.zip'
            directory_zip(contents_path, zip_filename=zip_filename, dir_cleanup=True)

        return download_success, errors


def get_max_date_from_items(items, date_key='modifiedTime', date_format=MODIFIED_DATE_FORMAT):
    max_date = None
    if items:
        if isinstance(items[0], list): #list of lists
            max_dates = []
            for sub_items in items:
                items_date = get_max_date_from_items(sub_items, date_key=date_key)
                if items_date:
                    max_dates.append(items_date)
            if max_dates:
                max_date = max(max_dates)
        else:
            timestamps = [i[date_key] for i in items if i[date_key]]
            if timestamps:
                max_date = max([
                    dt.datetime.strptime(t, date_format)
                    for t in timestamps])
    return max_date


def get_size_bytes_from_items(items, size_key='size'):
    size_bytes = None
    if items:
        if isinstance(items[0], list): #list of lists
            sizes = []
            for sub_items in items:
                items_size = get_size_bytes_from_items(sub_items, size_key=size_key)
                if items_size:
                    sizes.append(items_size)
            if sizes:
                size_bytes = sum(sizes)
        else:
            try:
                sizes = [i.get(size_key, '') for i in items if i.get(size_key, '')]
            except Exception as e:
                print(f"ERROR. Error when attempting to extract file property 'size' from {items}")
                raise e
            if sizes:
                size_bytes = sum([int(b) for b in sizes])
    return size_bytes


def write_to_file(data, filepath, file_type):
    with open(filepath, 'w') as f:
        if file_type == 'json':
            json.dump(data, f, indent=JSON_INDENT)
        elif file_type == 'text':
            f.write(data)
        f.close()


def directory_zip(dir_path, zip_filename='', dir_cleanup=False):
    if not zip_filename:
        zip_filename = f'{dir_path}.zip'

    #zip the files
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, 'w') as zipf:
        for root, dirs, files in os.walk(dir_path):
            for folder in dirs:
                folder_path = os.path.join(root, folder)
                zipf.write(
                    folder_path,
                    os.path.relpath(folder_path, dir_path)
                )
            for f in files:
                file_path = os.path.join(root, f)
                zipf.write(
                    file_path,
                    os.path.relpath(file_path, dir_path)
                )

    with open(zip_filename, 'wb') as f:
        f.write(zip_bytes.getvalue())
        f.close()
        zip_bytes.close()

    if dir_cleanup:
        shutil.rmtree(dir_path, ignore_errors=True)

#-----------------------------------------------------------------------------
# END
#-----------------------------------------------------------------------------