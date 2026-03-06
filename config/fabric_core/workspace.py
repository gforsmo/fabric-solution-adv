"""Workspace management module for Fabric CLI."""

import re
import time
import json
from .utils import get_fabric_cli_path, run_command


def workspace_exists(workspace_name):
    """Check if a Fabric workspace exists."""
    return run_command([get_fabric_cli_path(), 'ls', f'{workspace_name}.Workspace']).returncode == 0


def get_workspace_id(workspace_name):
    """Get the UUID of a workspace by name."""
    response = run_command(
        [get_fabric_cli_path(), 'get', f'{workspace_name}.Workspace', '-q', 'id'])
    if response.returncode == 0:
        workspace_id = response.stdout.strip()
        if workspace_id:
            return workspace_id
    uuid_match = re.search(
        r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', response.stdout.lower())
    return uuid_match.group() if uuid_match else None


def create_workspace(workspace_config):
    """
    Create a Fabric workspace.

    Args:
        workspace_config: Dict with 'name' and 'capacity' keys

    Returns:
        str: Workspace ID if successful, None otherwise
    """
    workspace_name  = workspace_config['name']
    capacity_name   = workspace_config.get('capacity')

    print(f"      Capacity name: {capacity_name}")

    if workspace_exists(workspace_name):
        print(f"      [SKIP] '{workspace_name}' already exists")
        return get_workspace_id(workspace_name)

    result = run_command([get_fabric_cli_path(), 'create',
                          f'{workspace_name}.Workspace',
                          '-P', f'capacityName={capacity_name}'])

    if result.returncode != 0:
        print(f"ERROR: Failed to create {workspace_name}")
        print(f"  stdout: {result.stdout}")
        print(f"  stderr: {result.stderr}")
        return None

    print(f"      Created '{workspace_name}'")
    time.sleep(5)

    workspace_id = get_workspace_id(workspace_name)
    if not workspace_id:
        print(f"      [WARN] Workspace created but ID could not be retrieved")

    return workspace_id


def get_workspace_role_assignments(workspace_id):
    """Get existing role assignments for a workspace."""
    result = run_command([get_fabric_cli_path(), 'api', '-X', 'get',
                          f'workspaces/{workspace_id}/roleAssignments'])

    if result.returncode != 0:
        return {}

    try:
        response_json = json.loads(result.stdout)
        if response_json.get('status_code') == 200:
            assignments = response_json.get('text', {}).get('value', [])
            return {assignment['principal']['id']: assignment['role']
                    for assignment in assignments}
    except (json.JSONDecodeError, KeyError):
        pass

    return {}


def assign_permissions(workspace_id, permissions, security_groups):
    """
    Assign permissions to security groups for a workspace.

    Args:
        workspace_id: Workspace UUID
        permissions: List of dicts with 'group' and 'role' keys
        security_groups: Dict mapping group names to group IDs
    """
    time.sleep(10)

    existing_assignments = get_workspace_role_assignments(workspace_id)

    for permission in permissions:
        group_id     = security_groups.get(permission.get('group'))
        desired_role = permission.get('role')

        if group_id in existing_assignments:
            existing_role = existing_assignments[group_id]
            if existing_role == desired_role:
                print(f"      [SKIP] {permission['group']} already has {desired_role} role")
            else:
                print(f"      [WARN] {permission['group']} already has {existing_role} role (wanted {desired_role})")
            continue

        request_body = {
            "principal": {
                "id": group_id,
                "type": "Group",
                "groupDetails": {"groupType": "SecurityGroup"}
            },
            "role": desired_role
        }

        assign_response = run_command([get_fabric_cli_path(), 'api', '-X', 'post',
                                       f'workspaces/{workspace_id}/roleAssignments',
                                       '-i', json.dumps(request_body)])

        if not assign_response.stdout.strip():
            print(f"      [FAIL] Empty response assigning {desired_role} to {permission['group']}")
            print(f"             stderr: {assign_response.stderr}")
            continue

        try:
            response_json = json.loads(assign_response.stdout)
        except json.JSONDecodeError:
            print(f"      [FAIL] Invalid JSON assigning {desired_role} to {permission['group']}")
            print(f"             stdout: {assign_response.stdout}")
            continue

        if response_json.get('status_code') in [200, 201]:
            print(f"      [OK]  Assigned {desired_role} to {permission['group']}")
        else:
            print(f"      [FAIL] Failed to assign {desired_role} to {permission['group']}")
            print(f"             Status  : {response_json.get('status_code')}")
            print(f"             Response: {response_json.get('text', {})}")
