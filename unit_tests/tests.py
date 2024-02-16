import json
from clouddata import integrations
from clouddata.gdrive import GDriveClient


TESTS = [
    'test_001_gdrive_login',
    'test_002_list_subfolders',
    'test_003_list_files_in_folder',
    'test_004_list_native_files_in_folder'
    'test_005_list_non_native_files_in_folder',
    'test_006_get_leaf_directory_config',
    'test_007_get_L0_directory_config'
]
L0_FOLDER = '03 Finances'
LEAF_FOLDER_ID = '1TsMGximJs_k2ip-D7rXrLTZJvIfRI1xD'
LEAF_FOLDER_NAME = '05 kpi_records'
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
        try:
            config = gdclient.get_directory_config(LEAF_FOLDER_NAME, folder_id=LEAF_FOLDER_ID)
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


def client_login():
    global gdclient
    if not gdclient:
        gdclient = GDriveClient()
        gdclient.login()


if __name__ == '__main__':
    run()
