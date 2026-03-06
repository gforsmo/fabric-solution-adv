"""Git integration module for connecting Fabric workspaces to GitHub."""

import os
import json
from .utils import get_fabric_cli_path, run_command


def get_or_create_git_connection(workspace_id, git_config):
    """
    Get existing or create new GitHub connection.
    Matches existing connections by GitHub URL (not name) for robustness.

    Args:
        workspace_id: Workspace UUID (used for connection creation context)
        git_config: Dict with 'organization' and 'repository' keys

    Returns:
        str: Connection ID if successful, None otherwise
    """
    owner_name = git_config.get('organization')
    repo_name  = git_config.get('repository')
    github_url = f"https://github.com/{owner_name}/{repo_name}"
    connection_name = f"GitHub-{owner_name}-{repo_name}"

    # Check for existing connection by URL match
    list_response = run_command([get_fabric_cli_path(), 'api', '-X', 'get', 'connections'])

    try:
        list_json = json.loads(list_response.stdout)
    except json.JSONDecodeError:
        print(f"      [WARN] Could not parse connections list")
        list_json = {}

    if list_json.get('status_code') == 200:
        connections = list_json.get('text', {}).get('value', [])
        print(f"      Found {len(connections)} existing connection(s)")
        for conn in connections:
            path = conn.get('connectionDetails', {}).get('path', '')
            name = conn.get('displayName', '')
            print(f"      Checking connection: {name} — path: {path}")
            if owner_name in path and repo_name in path:
                print(f"      [OK]  Matched existing connection: {name} (ID: {conn.get('id')})")
                return conn.get('id')

    print(f"      No matching connection found — creating new one...")

    # Create new connection
    request_body = {
        "connectivityType": "ShareableCloud",
        "displayName": connection_name,
        "connectionDetails": {
            "type": "GitHubSourceControl",
            "creationMethod": "GitHubSourceControl.Contents",
            "parameters": [{"dataType": "Text", "name": "url", "value": github_url}]
        },
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": "NotEncrypted",
            "skipTestConnection": False,
            "credentials": {
                "credentialType": "Key",
                "key": os.getenv('GH_PAT')
            }
        }
    }

    print(f"      Creating connection: {connection_name}")
    print(f"      URL: {github_url}")

    create_response = run_command([
        get_fabric_cli_path(), 'api', '-X', 'post', 'connections',
        '-i', json.dumps(request_body)
    ])

    try:
        create_json = json.loads(create_response.stdout)
    except json.JSONDecodeError:
        print(f"      [FAIL] Could not parse create connection response")
        print(f"             stdout: {create_response.stdout[:500]}")
        return None

    print(f"      Create response status: {create_json.get('status_code')}")

    if create_json.get('status_code') in [200, 201]:
        connection_id = create_json.get('text', {}).get('id')
        print(f"      [OK]  Created connection: {connection_name} (ID: {connection_id})")
        return connection_id

    print(f"      [FAIL] Failed to create connection")
    print(f"             Response: {create_json.get('text', {})}")
    return None


def update_workspace_from_git(workspace_id, workspace_name):
    """
    Update workspace content from Git (pull from Git).
    """
    status_response = run_command([
        get_fabric_cli_path(), 'api', '-X', 'get',
        f'workspaces/{workspace_id}/git/status'
    ])

    if not status_response.stdout.strip():
        print(f"  Warning: Failed to get Git status")
        return False

    try:
        status_json = json.loads(status_response.stdout)
        status_code = status_json.get('status_code')

        if status_code == 400:
            error_text = status_json.get('text', {})
            if error_text.get('errorCode') == 'WorkspaceGitConnectionNotInitialized':
                print(f"  -> Initializing Git connection")
                run_command([
                    get_fabric_cli_path(), 'api', '-X', 'post',
                    f'workspaces/{workspace_id}/git/initializeConnection',
                    '-i', '{}'
                ])
                status_response = run_command([
                    get_fabric_cli_path(), 'api', '-X', 'get',
                    f'workspaces/{workspace_id}/git/status'
                ])
                status_json = json.loads(status_response.stdout)
                status_code = status_json.get('status_code')

        if status_code != 200:
            error_text = status_json.get('text', {})
            print(f"  Warning: Failed to get Git status: {status_code}")
            print(f"     Error: {error_text}")
            return False

        git_status = status_json.get('text', {})
        remote_commit_hash = git_status.get('remoteCommitHash')
        workspace_head     = git_status.get('workspaceHead')

        if not remote_commit_hash:
            print(f"  Warning: No remoteCommitHash found in status")
            return False

    except json.JSONDecodeError:
        print(f"  Warning: Failed to parse Git status")
        return False

    update_request = {
        "remoteCommitHash": remote_commit_hash,
        "workspaceHead": workspace_head,
        "conflictResolution": {
            "conflictResolutionType": "Workspace",
            "conflictResolutionPolicy": "PreferWorkspace"
        },
        "options": {"allowOverrideItems": True}
    }

    update_response = run_command([
        get_fabric_cli_path(), 'api', '-X', 'post',
        f'workspaces/{workspace_id}/git/updateFromGit',
        '-i', json.dumps(update_request)
    ])

    if not update_response.stdout.strip():
        return True

    try:
        response_json = json.loads(update_response.stdout)
        status_code = response_json.get('status_code')
        if status_code in [200, 201, 202]:
            print(f"   Updated {workspace_name} from Git")
            return True
        print(f"  Warning: Update from Git failed (HTTP {status_code})")
        print(f"     Response: {response_json.get('text', {})}")
        return False
    except json.JSONDecodeError:
        print(f"  Warning: Update from Git returned invalid response")
        print(f"     stdout: {update_response.stdout[:500]}")
        return False


def connect_workspace_to_git(workspace_id, workspace_name, directory_name, git_config, connection_id):
    """
    Connect a Fabric workspace to a Git repository.

    Args:
        workspace_id: Workspace UUID
        workspace_name: Workspace display name (for logging)
        directory_name: Directory path in the repository
        git_config: Dict with 'organization', 'provider', 'repository', 'branch'
        connection_id: GitHub connection ID

    Returns:
        bool: True if successful, False otherwise
    """
    # Check if workspace is already connected to Git
    status_response = run_command([
        get_fabric_cli_path(), 'api', '-X', 'get',
        f'workspaces/{workspace_id}/git/status'
    ])

    if status_response.stdout.strip():
        try:
            status_json = json.loads(status_response.stdout)
            if status_json.get('status_code') == 200:
                git_status = status_json.get('text', {})
                if git_status.get('gitConnectionState') or git_status.get('remoteCommitHash'):
                    print(f"      {workspace_name} already connected to Git")
                    return True
        except json.JSONDecodeError:
            pass

    # Connect workspace to Git
    request_body = {
        "gitProviderDetails": {
            "ownerName": git_config.get('organization'),
            "gitProviderType": git_config.get('provider'),
            "repositoryName": git_config.get('repository'),
            "branchName": git_config.get('branch'),
            "directoryName": directory_name
        },
        "myGitCredentials": {
            "source": "ConfiguredConnection",
            "connectionId": connection_id
        }
    }

    print(f"      Connecting to: {git_config.get('repository')}/{git_config.get('branch')}/{directory_name}")
    print(f"      Connection ID: {connection_id}")

    connect_response = run_command([
        get_fabric_cli_path(), 'api', '-X', 'post',
        f'workspaces/{workspace_id}/git/connect',
        '-i', json.dumps(request_body)
    ])

    if not connect_response.stdout.strip():
        print(f"      [FAIL] Empty response connecting {workspace_name} to Git")
        return False

    try:
        connect_json = json.loads(connect_response.stdout)
    except json.JSONDecodeError:
        print(f"      [FAIL] Invalid JSON connecting {workspace_name} to Git")
        return False

    status_code = connect_json.get('status_code')
    print(f"      Connect response status: {status_code}")

    if status_code in [200, 201]:
        print(f"      [OK]  Connected {workspace_name} to Git")
        return True

    print(f"      [FAIL] Failed to connect {workspace_name} to Git")
    print(f"             Response: {connect_json.get('text', {})}")
    return False
