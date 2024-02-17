import os
import json
import datetime as dt


DATA_FILE_DEFAULT = 'data.zip'
DIRECTORY_FILE_DEFAULT = 'directory.json'
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
    'size_MB': '',
    'directory_file': DIRECTORY_FILE_DEFAULT,
    'data_file': DATA_FILE_DEFAULT,
    'security_class': '',
    'permissions': '',
    'expiration_date': ''
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

    def save(self):
        """save archive to local files to path_local/archive_id
        """
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
