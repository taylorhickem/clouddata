""" this module uses the Google Drive API to
 perform file management operations on a Google Drive account
 for details refer to Developer documentation
 https://developers.google.com/drive/api/guides/about-files
"""
#-----------------------------------------------------------------------------
# import dependencies
#-----------------------------------------------------------------------------
import io
import os
import json
import datetime as dt
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


#-----------------------------------------------------------------------------
# module variables
#-----------------------------------------------------------------------------
SCOPES = ['https://www.googleapis.com/auth/drive']
CLIENT_SECRET_DEFAULT = 'client_secret.json'
ordRef = {'A': 65}
FIELDS_DEFAULT = 'nextPageToken, files(kind, {mime_type}, id, name, {last_modified})'
MODIFIED_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
CONFIG_DIR = ''
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

    def download_file(self, file_id):
        #returns the file as a bytes type
        payload = False
        request = self.service.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            #print(F'Download {int(status.progress() * 100)}.')

        payload = file.getvalue()
        return payload

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

    def get_directory_config(self, folder_name, folder_id=''):
        last_modified_field = DRIVE_PARAMETERS['last_modified']
        config = DIR_CONFIG_DEFAULT.copy()
        del config['last_modified']
        config[last_modified_field] = ''
        config['name'] = folder_name

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

        modified_times = []
        for files in [native_files, non_native_files, sub_configs]:
            max_date = get_max_date_from_items(files, date_key=last_modified_field)
            if max_date:
                modified_times.append(max_date)
        if modified_times:
            max_date = max(modified_times)
        if max_date:
            config[last_modified_field] = dt.datetime.strftime(max_date, MODIFIED_DATE_FORMAT)

        return config

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

    def download_files_in_folder(self, folder_name, folder_id, mime_type=None):
        payloads = []
        names = []
        file_references = self.get_files_in_folder(
            folder_name, folder_id,
            include_subfolders=False,
            mime_type=mime_type)
        if len(file_references) > 0:
            for f in file_references:
                payload = self.download_file(f['id'])
                payloads.append(payload)
                names.append(f['name'])

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


def get_max_date_from_items(items, date_key='modifiedTime', date_format=MODIFIED_DATE_FORMAT):
    max_date = None
    if items:
        timestamps = [i[date_key] for i in items if i[date_key]]
        if timestamps:
            max_date = max([
                dt.datetime.strptime(t, MODIFIED_DATE_FORMAT)
                for t in timestamps])
    return max_date

#-----------------------------------------------------------------------------
# END
#-----------------------------------------------------------------------------