from httprequest_blueprints import execute_request
import argparse
import os
import json
import sys
import pickle
import re


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', dest='url', required=True)
    parser.add_argument('--sync-run-id', dest='sync_run_id', required=False)
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


def get_sync_details(
        trigger_url,
        sync_run_id,
        folder_name,
        file_name=f'sync_details_response.json'):
    sync_url = re.sub(
        r'syncs\/\d+\/trigger',
        f'sync_runs/{sync_run_id}',
        trigger_url)
    print(f'Grabbing run details for run {sync_run_id}.')
    sync_details_req = execute_request.execute_request(
        'GET', sync_url, headers={})
    sync_details_response = json.loads(sync_details_req.text)
    execute_request.create_folder_if_dne(folder_name)
    combined_name = execute_request.combine_folder_and_file_name(
        folder_name, file_name)
    write_json_to_file(sync_details_response, combined_name)
    return sync_details_response


def determine_run_status(
        sync_details_response,
        sync_run_id,
        failure_threshold,
        invalid_threshold):
    error_message = sync_details_response['data']['error_message']
    if sync_details_response['data']['status'] == 'completed':
        failed_records = sync_details_response['data'][
            'records_failed'] if sync_details_response['data']['records_failed'] is not None else 0
        invalid_records = sync_details_response['data'][
            'records_invalid'] if sync_details_response['data']['records_invalid'] is not None else 0

        if failed_records > failure_threshold:
            print(
                f'Census reports that {failed_records} records failed, which is over your threshold of {failure_threshold}.')
            exit_code = 1
        elif invalid_records > invalid_threshold:
            print(
                f'Census reports that {invalid_records} records failed, which is over your threshold of {invalid_threshold}.')
            exit_code = 2
        else:
            print(f'Census reports that run {sync_run_id} was successful.')
            exit_code = 0
    elif sync_details_response['data']['status'] == 'failed':
        print(f'Census reports that run {sync_run_id} failed.')
        print(f'Reason: {error_message}')
        exit_code = 1
    else:
        print(
            f'Census reports that the sync run {sync_run_id} is not yet completed.')
        exit_code = 255
    return exit_code


def main():
    args = get_args()
    invalid_threshold = int(args.invalid_threshold)
    failure_threshold = int(args.failure_threshold)
    trigger_url = args.url
    sync_id = trigger_url.split('/')[-2]

    artifact_directory_default = f'{os.environ.get("USER")}-artifacts'
    base_folder_name = execute_request.clean_folder_name(
        f'{os.environ.get("SHIPYARD_ARTIFACTS_DIRECTORY",artifact_directory_default)}/census-blueprints/')

    pickle_folder_name = execute_request.clean_folder_name(
        f'{base_folder_name}/variables')
    execute_request.create_folder_if_dne(pickle_folder_name)
    pickle_file_name = execute_request.combine_folder_and_file_name(
        pickle_folder_name, 'sync_run_id.pickle')

    if args.sync_run_id:
        sync_run_id = args.sync_run_id
    else:
        with open(pickle_file_name, 'rb') as f:
            sync_run_id = pickle.load(f)

    sync_details_response = get_sync_details(
        trigger_url,
        sync_run_id,
        folder_name=f'{base_folder_name}/responses',
        file_name=f'run_{sync_run_id}_response.json')
    sys.exit(
        determine_run_status(
            sync_details_response,
            sync_run_id,
            failure_threshold,
            invalid_threshold))


if __name__ == '__main__':
    main()
