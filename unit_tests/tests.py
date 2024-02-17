import os
import json
from clouddata import integrations
from clouddata.gdrive import GDriveClient
from clouddata.archive import DirectoryArchive


TESTS = [
    'test_001_gdrive_login',
    'test_002_list_subfolders',
    'test_003_list_files_in_folder',
    'test_004_list_native_files_in_folder'
    'test_005_list_non_native_files_in_folder',
    'test_006_get_leaf_directory_config',
    'test_007_get_L0_directory_config',
    'test_008_leaf_folder_archive',
    'test_009_archive_create'
    'test_010_file_download'
]
L0_FOLDER = '03 Finances'
LEAF_FOLDER_ID = '0Bzcklfmy0P60QjdqQ3NqOFZvQ00'
LEAF_FOLDER_NAME = '08 Interactive Brokers'
#LEAF_FOLDER_ID = '1TsMGximJs_k2ip-D7rXrLTZJvIfRI1xD'
#LEAF_FOLDER_NAME = '05 kpi_records'
FILE_ID = '1GblQkHIc2bTQAoxfxfCx-DI8QvB4ai5h'
FILENAME = 'KPIRcds.csv'
TEST_RESULTS = {}
gdclient = None


def run():
    run_tests()


def run_tests():
    global TEST_RESULTS
    for t in TESTS:
        test_result = run_test(t)
        TEST_RESULTS[t] = test_result
    write_results()


def write_results():
    with open('test_results.json', 'w') as f:
        json.dump(TEST_RESULTS, f)
        f.close()


def run_test(test_id, **args):
    test_result = globals()[test_id](**args)
    return test_result


def test_001_gdrive_login():
    global gdclient
    test_result = {}
    test_success = False
    errors = ''
    data = {}
    if not gdclient:
        try:
            client_login()
            test_success = True
            data = {'message': 'login success'}
        except Exception as e:
            errors = f'ERROR. GDrive client failed to load. {str(e)}'
    else:
        test_success = True
    test_result['success'] = test_success
    if data:
        test_result['data'] = data
    if errors:
        test_result['errors'] = errors
    return test_result


def test_002_list_subfolders():
    test_result = {}
    test_success = False
    data = {}
    errors = ''
    client_login()
    if gdclient:
        try:
            subfolders = gdclient.get_subfolders(folder_name=L0_FOLDER)
            test_success = len(subfolders) > 0
            if test_success:
                data = {
                    'folder_name': L0_FOLDER,
                    'subfolders': subfolders
                }
        except Exception as e:
            errors = f'ERROR. Failed to fetch subfolders. {str(e)}'
    else:
        errors = 'GDrive client not loaded.'
    test_result['success'] = test_success
    if data:
        test_result['data'] = data
    if errors:
        test_result['errors'] = errors
    return test_result


def test_003_list_files_in_folder():
    test_result = {}
    test_success = False
    data = {}
    errors = ''
    client_login()
    if gdclient:
        try:
            files = gdclient.get_files_in_folder(folder_id=LEAF_FOLDER_ID)
            test_success = len(files) > 0
            if test_success:
                data = {
                    'folder_id': LEAF_FOLDER_ID,
                    'files': files
                }
        except Exception as e:
            errors = f'ERROR. Failed to fetch folder files. {str(e)}'
    else:
        errors = 'GDrive client not loaded.'
    test_result['success'] = test_success
    if data:
        test_result['data'] = data
    if errors:
        test_result['errors'] = errors
    return test_result


def test_004_list_native_files_in_folder():
    query_alias = 'native_files_in_folder'
    test_result = {}
    test_success = False
    data = {}
    errors = ''
    client_login()
    if gdclient:
        try:
            files = gdclient.query_contents(query_alias=query_alias, folder_id=LEAF_FOLDER_ID)
            test_success = len(files) > 0
            if test_success:
                data = {
                    'folder_id': LEAF_FOLDER_ID,
                    'files': files
                }
        except Exception as e:
            errors = f'ERROR. Failed to fetch native folder files. {str(e)}'
    else:
        errors = 'GDrive client not loaded.'
    test_result['success'] = test_success
    if data:
        test_result['data'] = data
    if errors:
        test_result['errors'] = errors
    return test_result


def test_005_list_non_native_files_in_folder():
    query_alias = 'non_native_files_in_folder'
    test_result = {}
    test_success = False
    data = {}
    errors = ''
    client_login()
    if gdclient:
        try:
            files = gdclient.query_contents(query_alias=query_alias, folder_id=LEAF_FOLDER_ID)
            test_success = len(files) > 0
            if test_success:
                data = {
                    'folder_id': LEAF_FOLDER_ID,
                    'files': files
                }
        except Exception as e:
            errors = f'ERROR. Failed to fetch native folder files. {str(e)}'
    else:
        errors = 'GDrive client not loaded.'
    test_result['success'] = test_success
    if data:
        test_result['data'] = data
    if errors:
        test_result['errors'] = errors
    return test_result


def test_006_get_leaf_directory_config():
    test_result = {}
    test_success = False
    data = {}
    errors = ''
    client_login()
    if gdclient:
        #try:
        config = gdclient.get_directory_config(LEAF_FOLDER_NAME, folder_id=LEAF_FOLDER_ID)
        test_success = len(config) > 0
        if test_success:
            data = config
        #except Exception as e:
        #   errors = f'ERROR. Failed to fetch directory config. {str(e)}'
    else:
        errors = 'GDrive client not loaded.'
    test_result['success'] = test_success
    if data:
        test_result['data'] = data
    if errors:
        test_result['errors'] = errors
    return test_result


def test_007_get_L0_directory_config():
    test_result = {}
    test_success = False
    data = {}
    errors = ''
    client_login()
    if gdclient:
        try:
            config = gdclient.get_directory_config(L0_FOLDER)
            test_success = len(config) > 0
            if test_success:
                data = config
        except Exception as e:
            errors = f'ERROR. Failed to fetch directory config. {str(e)}'
    else:
        errors = 'GDrive client not loaded.'
    test_result['success'] = test_success
    if data:
        test_result['data'] = data
    if errors:
        test_result['errors'] = errors
    return test_result


def test_008_leaf_folder_archive():
    directory_name = LEAF_FOLDER_NAME
    config_path = ''
    test_result = {}
    test_success = False
    errors = ''
    client_login()
    if gdclient:
        #try:
        config_path = gdclient.get_directory_config(
            directory_name, folder_id=LEAF_FOLDER_ID, save_to_file=True)
        test_success = len(config_path) > 0
        if test_success:
            test_result['directory_config_file'] = config_path
        #except Exception as e:
        #    errors = f'ERROR. Failed to fetch directory config. {str(e)}'
        if test_success:
            with open(config_path, 'r') as f:
                config = json.load(f)
                f.close()
            #try:
            payload_path, errors = gdclient.directory_download(directory_name,
                folder_id=LEAF_FOLDER_ID, zip_filename='data.zip', as_zip=True, config=config)
            test_success = len(errors) == 0
            if test_success:
                test_result['directory_data_file'] = payload_path
            #except Exception as e:
            #    errors = f'ERROR. Failed to download directory. {str(e)}'
        if test_success:
            try:
                archive = DirectoryArchive(
                    data_file='data.zip',
                    #data_file=payload_path,
                    directory_file=config_path,
                    name=directory_name
                )
                archive.save()
                test_success = True
            except Exception as e:
                error_msg = f'ERROR. Failed to save archive. {str(e)}'
                if errors:
                    errors = errors + '\n' + error_msg
                else:
                    errors = error_msg
    else:
        errors = 'GDrive client not loaded.'
    test_result['success'] = test_success
    if errors:
        test_result['errors'] = errors
    return test_result


def test_009_archive_create():
    data_file = 'data.zip'
    directory_file = 'directory.json'
    directory_name = 'test'
    test_result = {}
    test_success = False
    data = {}
    errors = ''
    try:
        archive = DirectoryArchive(
            data_file=data_file,
            directory_file=directory_file,
            name=directory_name
        )
        archive.save()
        data = archive.id
        test_success = True
    except Exception as e:
        errors = f'ERROR. Failed to save archive. {str(e)}'
    test_result['success'] = test_success
    if data:
        test_result['data'] = data
    if errors:
        test_result['errors'] = errors
    return test_result


def test_010_file_download():
    filename = FILENAME
    file_id = FILE_ID
    test_result = {}
    test_success = False
    errors = ''
    client_login()
    if gdclient:
        try:
            download_success, errors = gdclient.file_download(
                file_id,
                filename,
                return_bytes=False
            )
            if download_success:
                test_success = os.path.exists(filename)
                if test_success:
                    test_result['downloaded_file'] = filename
                else:
                    errors = f'ERROR. Failed to download file {filename}. {errors}'
        except Exception as e:
            errors = f'ERROR. Failed to download file {filename}. {str(e)}'
    else:
        errors = 'GDrive client not loaded.'
    test_result['success'] = test_success
    if errors:
        test_result['errors'] = errors
    return test_result


def client_login():
    global gdclient
    if not gdclient:
        gdclient = GDriveClient()
        gdclient.login()


if __name__ == '__main__':
    run()
