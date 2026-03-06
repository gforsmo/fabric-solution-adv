# Please note: the code in this Python file has intentionally been written WITHOUT things like:
# testing, logging, error-handling, validation, documentation, comments etc
# for now I'm trying to make it as simple as possible to follow the logic
# In future weeks, we'll refactor the code to make it more robust!
import os
import sys
import time
from fabric_core import (
    auth, bootstrap, create_workspace, assign_permissions,
    get_or_create_git_connection, connect_workspace_to_git,
    create_capacity, suspend_capacity
)
from fabric_core.utils import load_config

# Ensure UTF-8 encoding for stdout to support Unicode characters (like checkmarks)
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


def main():
    total_start = time.time()
    bootstrap()

    config = load_config(
        os.getenv('CONFIG_FILE', 'config/templates/v01/v01-template.yml'))

    log_section("AUTHENTICATING")
    if not auth():
        log_fail("Authentication failed. Cannot proceed.")
        return
    log_ok("Authenticated successfully")

    azure_config      = config['azure']
    subscription_id   = azure_config['subscription_id']
    capacity_defaults = azure_config.get('capacity_defaults', {})
    security_groups   = azure_config.get('security_groups', {})
    git_config        = config.get('github', {})

    log_info(f"Subscription  : {subscription_id}")
    log_info(f"Resource group: {capacity_defaults.get('resource_group')}")
    log_info(f"Region        : {capacity_defaults.get('region')}")
    log_info(f"SKU           : {capacity_defaults.get('sku')}")

    # ── Capacities ───────────────────────────────────────────────────────────
    capacities = config.get('capacities', [])
    log_section(f"CREATING CAPACITIES  ({len(capacities)} total)")

    for capacity_config in capacities:
        resource_group = capacity_config.get(
            'resource_group', capacity_defaults.get('resource_group'))
        log_step(f"Capacity: {capacity_config['name']}  (rg: {resource_group})")
        t = time.time()
        result = create_capacity(capacity_config, subscription_id,
                                 resource_group, capacity_defaults)
        if result:
            log_ok(f"Done ({elapsed(t)})")
        else:
            log_fail(f"Failed ({elapsed(t)})")

    # ── Workspaces ───────────────────────────────────────────────────────────
    workspaces = config.get('workspaces', [])
    log_section(f"CREATING WORKSPACES  ({len(workspaces)} total)")

    github_connection_id = None
    for workspace_config in workspaces:
        ws_name = workspace_config['name']
        log_step(f"Workspace: {ws_name}")
        t = time.time()

        workspace_id = create_workspace(workspace_config)

        if workspace_id:
            log_ok(f"Created — ID: {workspace_id} ({elapsed(t)})")
        else:
            log_fail(f"create_workspace returned None ({elapsed(t)})")

        if 'permissions' in workspace_config and workspace_id:
            log_info(f"Assigning {len(workspace_config['permissions'])} permission(s)...")
            t2 = time.time()
            assign_permissions(
                workspace_id, workspace_config['permissions'], security_groups)
            log_ok(f"Permissions assigned ({elapsed(t2)})")

        if 'connect_to_git_folder' in workspace_config and workspace_id and git_config:
            folder = workspace_config['connect_to_git_folder']
            log_info(f"Git folder: {folder}")

            if not github_connection_id:
                log_info("Getting/creating Git connection...")
                t2 = time.time()
                github_connection_id = get_or_create_git_connection(
                    workspace_id, git_config)
                if github_connection_id:
                    log_ok(f"Git connection ID: {github_connection_id} ({elapsed(t2)})")
                else:
                    log_fail(f"get_or_create_git_connection returned None ({elapsed(t2)})")

            if github_connection_id:
                log_info("Connecting workspace to Git...")
                t2 = time.time()
                connect_workspace_to_git(workspace_id, ws_name,
                                         folder, git_config, github_connection_id)
                log_ok(f"Connected to Git ({elapsed(t2)})")

    # ── Suspend capacities ───────────────────────────────────────────────────
    log_section("SUSPENDING CAPACITIES")
    log_info("Waiting 20 seconds before suspending...")
    time.sleep(20)

    for capacity_config in capacities:
        resource_group = capacity_config.get(
            'resource_group', capacity_defaults.get('resource_group'))
        log_step(f"Suspending: {capacity_config['name']}")
        t = time.time()
        suspend_capacity(capacity_config['name'], subscription_id, resource_group)
        log_ok(f"Done ({elapsed(t)})")

    log_section(f"DONE  (total time: {elapsed(total_start)})")


if __name__ == "__main__":
    main()
