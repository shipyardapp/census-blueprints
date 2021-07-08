from httprequest_blueprints import execute_request
import argparse
import os
import json
import pickle
import time
import sys

try:
    import check_sync_status
except BaseException:
    from . import check_sync_status


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', dest='url', required=True)
    parser.add_argument(
        '--check-status',
        dest='check_status',
        default='TRUE',
        choices={
            'TRUE',
            'FALSE'},
        required=False)
    parser.add_argument(
        '--failure-threshold',
        dest='failure_threshold',
        default=0,
        required=False)
    parser.add_argument(
        '--invalid-threshold',
        dest='invalid_threshold',
        default=0,
        required=False)
    args = parser.parse_args()
    return args


def write_json_to_file(json_object, file_name):
    with open(file_name, 'w') as f:
        f.write(
            json.dumps(
                json_object,
                ensure_ascii=False,
                indent=4))
    print(f'Response stored at {file_name}')


def execute_job(
        url,
        folder_name,
        file_name='job_details_response.json'):
    print(f'Starting sync for {url.split("/")[-2]}')
    job_run_req = execute_request.execute_request(
        'POST', url, headers={})
    job_run_response = json.loads(job_run_req.text)
    execute_request.create_folder_if_dne(folder_name)
    combined_name = execute_request.combine_folder_and_file_name(
        folder_name, file_name)
    write_json_to_file(
        job_run_response, combined_name)
    return job_run_response


def main():
    args = get_args()
    invalid_threshold = int(args.invalid_threshold)
    failure_threshold = int(args.failure_threshold)
    url = args.url
    check_status = execute_request.convert_to_boolean(args.check_status)

    sync_id = url.split('/')[-2]
    artifact_directory_default = f'{os.environ.get("USER")}-artifacts'
    base_folder_name = execute_request.clean_folder_name(
        f'{os.environ.get("SHIPYARD_ARTIFACTS_DIRECTORY",artifact_directory_default)}/census-blueprints/')

    job_run_response = execute_job(
        url,
        folder_name=f'{base_folder_name}/responses',
        file_name=f'sync_{sync_id}_response.json')

    sync_run_id = job_run_response['data']['sync_run_id']
    pickle_folder_name = execute_request.clean_folder_name(
        f'{base_folder_name}/variables')
    execute_request.create_folder_if_dne(pickle_folder_name)
    pickle_file_name = execute_request.combine_folder_and_file_name(
        pickle_folder_name, 'sync_run_id.pickle')
    with open(pickle_file_name, 'wb') as f:
        pickle.dump(sync_run_id, f)

    if check_status:
        is_complete = False
        # Census API returns null if you poll it too quickly the first time.
        time.sleep(5)
        while not is_complete:
            sync_details_response = check_sync_status.get_sync_details(
                url,
                sync_run_id,
                folder_name=f'{base_folder_name}/responses',
                file_name=f'run_{sync_run_id}_response.json')
            is_complete = sync_details_response['data'][
                'status'] == 'completed' or sync_details_response['data']['status'] == 'failed'
            if not is_complete:
                print(
                    f'Run {sync_run_id} is not complete. Waiting 30 seconds and trying again.')
                time.sleep(30)

        sys.exit(
            check_sync_status.determine_run_status(
                sync_details_response,
                sync_run_id,
                failure_threshold,
                invalid_threshold))


if __name__ == '__main__':
    main()
