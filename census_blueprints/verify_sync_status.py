import sys
import argparse
import requests
import shipyard_utils as shipyard


EXIT_CODE_STATUS_COMPLETED = 0
EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_BAD_REQUEST = 201
EXIT_CODE_UNKNOWN_ERROR = 211
EXIT_CODE_SYNC_CHECK_ERROR = 220
EXIT_CODE_STATUS_RUNNING = 222
EXIT_CODE_STATUS_FAILED = 223


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--sync-run-id', dest='sync_run_id', required=False)
    args = parser.parse_args()
    return args


def get_sync_status(sync_run_id, access_token):
    """
    Executes/starts a Census Sync
    """
    sync_post_api = f"https://bearer:{access_token}@app.getcensus.com/api/v1/sync_runs/{sync_run_id}"
    check_sync_response = {}
    try:
        check_sync_response = requests.get(sync_post_api)
        # check if successful, if not return error message
        if check_sync_response.status_code == requests.codes.ok:
            sync_run_json = check_sync_response.json()
        else:
            print(f"Sync check failed. Reason: {check_sync_response.content}")
            sys.exit(EXIT_CODE_BAD_REQUEST)
    except Exception as e:
        print(f"Check Sync Run {sync_run_id} failed due to: {e}")
        sys.exit(EXIT_CODE_BAD_REQUEST)
    
    if sync_run_json['status'] == 'success':
        print("Successfully managed to check sync")
        return sync_run_json['data']

    else:
        error_message = sync_run_json['data']['error_message']
        print(f"Sync run {sync_run_id} was unsuccessful. Reason: {error_message}")
        sys.exit(EXIT_CODE_SYNC_CHECK_ERROR)


def handle_sync_run_data(sync_run_data):
    """
    Analyses sync run data to determine status and print sync run information
    
    Returns:
        status_code: Exit Status code detailing sync status
    """
    status = sync_run_data['status']
    sync_id = sync_run_data['sync_id']
    status_code = EXIT_CODE_STATUS_COMPLETED
    if status == "completed":
        print(
            f"Sync {sync_id} completed successfully. ",
            f"Completed at: {sync_run_data['completed_at']}"
        )
        status_code = EXIT_CODE_STATUS_COMPLETED
        
    elif status == "working":
        print(
            f"Sync {sync_id} still Running. ",
            f"Current records processed: {sync_run_data['records_processed']}"
        )
        status_code = EXIT_CODE_STATUS_RUNNING
    
    elif status == "failed":
        error_code = sync_run_data['error_code']
        error_message = sync_run_data['error_message']
        print(f"Sync {sync_id} failed. {error_code} {error_message}")
        status_code = EXIT_CODE_STATUS_FAILED
        
    else:
        print("An unknown error has occured with {sync_id}")
        print("Unknown Sync status: {status}")
        status_code = EXIT_CODE_UNKNOWN_ERROR
    
    return status_code


def main():
    args = get_args()
    access_token = args.access_token  
    # create artifacts folder to save run id
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'census')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
    
    # get sync run id variable from user or pickle file if not inputted
    if args.sync_run_id:
        sync_run_id = args.sync_run_id
    else:
        sync_run_id = shipyard.logs.read_pickle_file(
            artifact_subfolder_paths, 'sync_run_id')
    # run check sync status
    sync_run_data = get_sync_status(sync_run_id, access_token)
    exit_code_status = handle_sync_run_data(sync_run_data)
    sys.exit(exit_code_status)


if __name__ == "__main__":
    main()


