"""
Create feature workspaces for development branches.

This script is designed to be called from GitHub Actions workflow_dispatch.
It creates workspaces for a feature branch and connects them to Git.
"""

import os
import sys
import time

from fabric_core import (
    auth, bootstrap, create_workspace, assign_permissions,
    get_or_create_git_connection, connect_workspace_to_git
)
from fabric_core.git_integration import update_workspace_from_git
from fabric_core.utils import load_config

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


def log_section(title):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")

def log_step(msg):
    print(f"\n  --> {msg}")

def log_ok(msg):
    print(f"      [OK]  {msg}")

def log_fail(msg):
    print(f"      [FAIL] {msg}")

def log_info(msg):
    print(f"      [INFO] {msg}")

def elapsed(start):
    return f"{time.time() - start:.1f}s"


def get_capacity_for_workspace_type(workspace_type, solution_version):
    """Determine which capacity to use based on workspace type."""
    capacity_map = {
        'processing':  f'fc{solution_version}devengineering',
        'datastores':  f'fc{solution_version}devengineering',
        'consumption': f'fc{solution_version}devconsumption'
    }
    return capacity_map.get(workspace_type)


def main():
    total_start = time.time()
    bootstrap()

    # Get inputs from environment (set by GitHub Actions workflow)
    feature_branch   = os.getenv('FEATURE_BRANCH_NAME')
    workspaces_input = os.getenv('WORKSPACES_TO_CREATE', 'processing,datastores')

    # Parse workspace types (comma-separated)
    workspace_types = [ws.strip() for ws in workspaces_input.split(',') if ws.strip()]

    # Load config
    config = load_config(
        os.getenv('CONFIG_FILE', 'config/templates/v01/v01-template.yml'))

    solution_version = config.get('solution_version', 'av01')
    azure_config     = config['azure']
    security_groups  = azure_config.get('security_groups', {})
    git_config       = config.get('github', {})

    # Override git branch to use feature branch
    git_config['branch'] = feature_branch

    log_section("AUTHENTICATING")
    if not auth():
        log_fail("Authentication failed. Cannot proceed.")
        return
    log_ok("Authenticated successfully")
    log_info(f"Feature branch  : {feature_branch}")
    log_info(f"Solution version: {solution_version}")
    log_info(f"Workspace types : {', '.join(workspace_types)}")

    log_section(f"CREATING FEATURE WORKSPACES  ({len(workspace_types)} total)")

    github_connection_id = None

    for workspace_type in workspace_types:
        workspace_name = f"{solution_version}-{feature_branch}-{workspace_type}"
        capacity_name  = get_capacity_for_workspace_type(workspace_type, solution_version)

        log_step(f"Workspace: {workspace_name}")

        if not capacity_name:
            log_fail(f"Unknown workspace type: {workspace_type}")
            continue

        log_info(f"Capacity : {capacity_name}")

        # ── Create workspace ─────────────────────────────────────────────────
        t = time.time()
        workspace_id = create_workspace({'name': workspace_name, 'capacity': capacity_name})

        if not workspace_id:
            log_fail(f"create_workspace returned None ({elapsed(t)})")
            continue
        log_ok(f"Created — ID: {workspace_id} ({elapsed(t)})")

        # ── Assign permissions ───────────────────────────────────────────────
        log_info("Assigning permissions...")
        t = time.time()
        assign_permissions(workspace_id, [{'group': 'sg-av-engineers', 'role': 'Admin'}], security_groups)
        log_ok(f"Permissions assigned ({elapsed(t)})")

        # ── Git connection ───────────────────────────────────────────────────
        if not github_connection_id:
            log_info("Getting/creating Git connection...")
            t = time.time()
            github_connection_id = get_or_create_git_connection(workspace_id, git_config)
            if github_connection_id:
                log_ok(f"Git connection ID: {github_connection_id} ({elapsed(t)})")
            else:
                log_fail(f"get_or_create_git_connection returned None ({elapsed(t)})")

        if not github_connection_id:
            log_fail("No Git connection — skipping Git steps")
            continue

        # ── Connect workspace to Git ─────────────────────────────────────────
        git_directory = f"solution/{workspace_type}/"
        log_info(f"Git folder: {git_directory}")
        t = time.time()
        connected = connect_workspace_to_git(
            workspace_id, workspace_name, git_directory, git_config, github_connection_id)

        if connected:
            log_ok(f"Connected to Git ({elapsed(t)})")
        else:
            log_fail(f"connect_workspace_to_git failed ({elapsed(t)})")
            continue

        # ── Sync content from Git ────────────────────────────────────────────
        log_info("Syncing content from Git...")
        t = time.time()
        synced = update_workspace_from_git(workspace_id, workspace_name)
        if synced:
            log_ok(f"Git sync done ({elapsed(t)})")
        else:
            log_fail(f"Git sync failed ({elapsed(t)})")

    log_section(f"DONE  (total time: {elapsed(total_start)})")


if __name__ == "__main__":
    main()
