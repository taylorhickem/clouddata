# clouddata
integration between different cloud data API services

## About
service to integrate cloud data providers

### Cloud data providers
* AWS S3
* Google Drive

### Google Dirve

#### setup
[using google drive API with Python and a service account: Medium Matheo Daly 2023](https://medium.com/@matheodaly.md/using-google-drive-api-with-python-and-a-service-account-d6ae1f6456c2)

1. [create a google service account <user>@<gcp_project>.iam.gserviceaccount.com](https://cloud.google.com/iam/docs/service-accounts-create)
2. [enable Google Drive API on your GCP project](https://cloud.google.com/endpoints/docs/openapi/enable-api#:~:text=In%20the%20Google%20Cloud%20console,APIs%20%26%20services%20for%20your%20project.&text=On%20the%20Library%20page%2C%20click,API%20you%20want%20to%20enable.)
3. [grant edit permissions](https://support.google.com/drive/answer/7166529?hl=en&co=GENIE.Platform%3DDesktop) to the GDrive folders you want the app to manage
4. get [client_secret.json](https://stackoverflow.com/questions/40136699/using-google-api-for-python-where-do-i-get-the-client-secrets-json-file-from) file and add it to root directory

### Methods
* get files
* get folders
* (In progress) GDrive -> S3 Folder Snapshot migration
* (Future) Gdrive -> S3 archive
* (Future) GDrive -> S3 Master-Slave Folder sync

### browse files and folders
[Google Drive API: search for files and folders](https://developers.google.com/drive/api/guides/search-files)

[video demo: browse GDrive files and folders](https://taylorhickem-media.s3.ap-southeast-1.amazonaws.com/videos/roles/life_hacks/projects/2024001_gdrive_to_s3/clouddata_demo.mp4)
- get folder_id from folder_name
- get files
- get subfolders

### GDrive->S3

#### Folder snapshot

1. scan directory recursively to get directory tree JSON
2. 

#### directory tree JSON

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

#### (FUTURE) Archive

#### (FUTURE) Folder sync