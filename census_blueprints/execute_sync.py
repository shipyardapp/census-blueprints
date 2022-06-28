import argparse
import sys
import requests
import shipyard_utils as shipyard


EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_BAD_REQUEST = 201
EXIT_CODE_SYNC_REFRESH_ERROR = 210
EXIT_CODE_UNKNOWN_ERROR = 211


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--access-token', dest='access_token', required=True)
    parser.add_argument('--sync-id', dest='sync_id', required=True)
    args = parser.parse_args()
    return args


def execute_sync(sync_id, access_token):
    """
    Executes/starts a Census Sync
    """
    sync_post_api = f"https://bearer:{access_token}@app.getcensus.com/api/v1/syncs/{sync_id}/trigger"
    api_headers = {
        'Content-Type': 'application/json'
    }
    sync_trigger_json = {}
    try:
        sync_trigger_response = requests.post(
            sync_post_api, headers=api_headers)
        # check if successful, if not return error message
        if sync_trigger_response.status_code == requests.codes.ok:
            sync_trigger_json = sync_trigger_response.json()
        elif sync_trigger_response.status_code == 404:
            print(f"Sync request failed. Check if sync ID {sync_id} is valid?")
            sys.exit(EXIT_CODE_BAD_REQUEST)
        else:
            print(
                f"Sync request failed. Reason: {sync_trigger_response.text}")
            if "Access denied" in sync_trigger_response.text:
                print(
                    'Check to make sure that your access token doesn\'t have any typos and includes "secret-token:"')
                sys.exit(EXIT_CODE_INVALID_CREDENTIALS)
            sys.exit(EXIT_CODE_BAD_REQUEST)
    except Exception as e:
        print(f"Sync trigger request failed due to: {e}")
        sys.exit(EXIT_CODE_BAD_REQUEST)

    if sync_trigger_json['status'] == 'success':
        print("Successfully triggered sync")
        return sync_trigger_response.json()

    if sync_trigger_json['status'] == 'error':
        print(
            f"Encountered an error - Census says: {sync_trigger_json['message']}")
        sys.exit(EXIT_CODE_SYNC_REFRESH_ERROR)
    else:
        print(
            f"An unknown error has occurred - API response: {sync_trigger_json}")
        sys.exit(EXIT_CODE_UNKNOWN_ERROR)


def main():
    args = get_args()
    access_token = args.access_token
    sync_id = args.sync_id

    # execute trigger sync
    trigger_sync = execute_sync(sync_id, access_token)
    sync_run_id = trigger_sync['data']['sync_run_id']

    # create artifacts folder to save run id
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'census')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)

    # save sync run id as variable
    shipyard.logs.create_pickle_file(artifact_subfolder_paths,
                                     'sync_run_id', sync_run_id)


if __name__ == "__main__":
    main()
