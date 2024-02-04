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
* (In progress) GDrive -> S3 Folder Snapshot migration
* (Future) Gdrive -> S3 archive
* (Future) GDrive -> S3 Master-Slave Folder sync

### GDrive->S3

#### Folder snapshot

to be updated ...

#### (FUTURE) Archive

#### (FUTURE) Folder sync