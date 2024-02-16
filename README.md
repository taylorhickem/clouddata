# clouddata
integration between different cloud data API services

## About
service to integrate cloud data providers

### Cloud data providers
* AWS S3
* Google Drive

### Integrations
* GDrive->S3 directory snapshot
* GDrive->S3 directory sync

### Google Dirve
[Google Drive API: search for files and folders](https://developers.google.com/drive/api/guides/search-files)

#### setup
[using google drive API with Python and a service account: Medium Matheo Daly 2023](https://medium.com/@matheodaly.md/using-google-drive-api-with-python-and-a-service-account-d6ae1f6456c2)

1. [create a google service account <user>@<gcp_project>.iam.gserviceaccount.com](https://cloud.google.com/iam/docs/service-accounts-create)
2. [enable Google Drive API on your GCP project](https://cloud.google.com/endpoints/docs/openapi/enable-api#:~:text=In%20the%20Google%20Cloud%20console,APIs%20%26%20services%20for%20your%20project.&text=On%20the%20Library%20page%2C%20click,API%20you%20want%20to%20enable.)
3. [grant edit permissions](https://support.google.com/drive/answer/7166529?hl=en&co=GENIE.Platform%3DDesktop) to the GDrive folders you want the app to manage
4. get [client_secret.json](https://stackoverflow.com/questions/40136699/using-google-api-for-python-where-do-i-get-the-client-secrets-json-file-from) file and add it to root directory

### Methods
* client login 
* get files
* get folders
* directory tree JSON

#### client login
```
from clouddata.gdrive import GDriveClient
...
gdclient = GDriveClient()
gdclient.login()
```
Ensure that the `client_secret.json` is placed in the root directory. 
At construction, the `GDriveClient` class instance will look for this file to authenticate the API session.

#### browse files and folders
- get folder_id from folder_name
- get files
- get subfolders
- built-in query
- custom query
 

[video demo: browse GDrive files and folders](https://taylorhickem-media.s3.ap-southeast-1.amazonaws.com/videos/roles/life_hacks/projects/2024001_gdrive_to_s3/clouddata_demo.mp4)
get folder_id from folder_name
```
folder_id = gdclient.get_folder_id('05 kpi_records')
```

get files
```
files = gdclient.get_files_in_folder(folder_id='1TsMGximJs_k2ip-D7rXrLTZJvIfRI1xD')
```

get subfolders
```
subfolders = gdclient.get_subfolders(folder_name='03 Finances')
```

built-in query
```
files = gdclient.query_contents(query_alias='native_files_in_folder', folder_id='1TsMGximJs_k2ip-D7rXrLTZJvIfRI1xD')
```

available built-in queries
```
folder_contents
files_in_folder
native_files_in_folder
non_native_files_in_folder
folders_in_folder
folders_only
files_only
```

custom query
```
files = gdclient.query_contents(qry_stm='name contains "DBS Visa"', folder_id='1TsMGximJs_k2ip-D7rXrLTZJvIfRI1xD')
```
You can also pass custom Google API queries using the optional argument `qry_stm` on the method `GDriveClient.query_contents()`
for details refer to the Google API documentation.

#### directory tree JSON
```
directory_config = gdclient.get_directory_config('05 kpi_records', folder_id='1TsMGximJs_k2ip-D7rXrLTZJvIfRI1xD')
```
The first positional argument, directory name, is a required argument 
ahd should match the actual directory name.

The directory tree is a JSON file of the nested lists of folders and files.
Each folder contains 
1. metadata fields [name, id, modifiedTime]
2. list of subfolders `folders`
3. list of native `native_files` and non-native `non_native_files` files

```
{
  name: <folder_name>,
  id: <folder_id>,
  modifiedTime: <last_modified_timestamp>,
  folders: [<recursive subfolders >],
  native_files: [],
  non_native_files: []
}
```

The reason for segregation of native and non-native files is because 
native files (Google Sheets, Docs, ...) require additional steps
to export into a standard format (Excel, CSV, TEXT, etc..)

### GDrive->S3 directory snapshot

1. scan directory recursively to get directory tree JSON
2. upload scan results to S3 bucket

#### (FUTURE) GDrive->S3 directory sync