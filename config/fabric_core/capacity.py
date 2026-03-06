"""Capacity management module for Azure Fabric capacities."""
import time
from .utils import call_azure_api, call_fabric_api


def get_capacity_id(capacity_name, subscription_id=None, resource_group=None):
    """Get Fabric capacity GUID by name using Fabric API."""
    status, response = call_fabric_api("/capacities")
    if status == 200:
        for cap in response.get('value', []):
            if cap.get('displayName', '').lower() == capacity_name.lower():
                cap_id = cap.get('id')
                print(f"      Found '{capacity_name}' — ID: {cap_id}")
                return cap_id
    print(f"      [WARN] Could not find '{capacity_name}' via Fabric API (status: {status})")
    return None


def capacity_exists(capacity_name, subscription_id, resource_group):
    """Check if a Fabric capacity exists in Azure."""
    status, _ = call_azure_api(
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version=2023-11-01")
    return status == 200


def create_capacity(capacity_config, subscription_id, resource_group, defaults):
    capacity_name = capacity_config['name']

    print(f"  --> Creating capacity: {capacity_name}")
    print(f"      Resource group : {resource_group}")
    print(f"      Region         : {capacity_config.get('region', defaults.get('region'))}")
    print(f"      SKU            : {capacity_config.get('sku', defaults.get('sku'))}")

    print(f"      Checking if capacity exists...")
    exists_status, exists_response = call_azure_api(
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version=2023-11-01")

    if exists_status == 200:
        print(f"      [SKIP] '{capacity_name}' already exists — resuming...")
        resume_status, _ = call_azure_api(
            f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/resume?api-version=2023-11-01", 'post')
        print(f"      Resume status: {resume_status}")
        capacity_id = get_capacity_id(capacity_name)
        print(f"      Capacity ID  : {capacity_id}")
        return capacity_id
    elif exists_status == 404:
        print(f"      Not found — will create.")
    else:
        print(f"      [WARN] Unexpected status: {exists_status} — {exists_response}")

    # Build request body
    admin_members = capacity_config.get('admin_members', defaults.get('capacity_admins', ''))
    admin_members = admin_members if isinstance(admin_members, list) else [
        admin_id.strip() for admin_id in admin_members.split(',') if admin_id.strip()]

    request_body = {
        "location": capacity_config.get('region', defaults.get('region')),
        "sku": {"name": capacity_config.get('sku', defaults.get('sku')), "tier": "Fabric"},
        "properties": {"administration": {"members": admin_members}}
    }
    print(f"      Admin members  : {admin_members}")
    print(f"      Sending PUT request...")

    status, response = call_azure_api(
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version=2023-11-01",
        'put', request_body)

    print(f"      Response status: {status}")

    if status in [200, 201]:
        print(f"      [OK] Created '{capacity_name}' — waiting 40s for provisioning...")
        time.sleep(40)
        capacity_id = get_capacity_id(capacity_name)
        print(f"      Capacity ID  : {capacity_id}")
        return capacity_id
    elif status == 202:
        print(f"      [OK] Accepted (async) '{capacity_name}' — waiting 60s...")
        time.sleep(60)
        capacity_id = get_capacity_id(capacity_name)
        print(f"      Capacity ID  : {capacity_id}")
        return capacity_id
    else:
        print(f"      [FAIL] Failed to create '{capacity_name}'")
        print(f"      Response body: {response}")
        return None


def suspend_capacity(capacity_name, subscription_id, resource_group):
    print(f"  --> Suspending capacity: {capacity_name}")

    for attempt in range(1, 6):
        print(f"      Attempt {attempt}/5...")
        status, response = call_azure_api(
            f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/suspend?api-version=2023-11-01",
            'post')
        print(f"      Response status: {status}")

        if status in [200, 202]:
            print(f"      [OK] Suspended '{capacity_name}'")
            return True

        print(f"      [WARN] Not suspended yet (status {status}) — {response}")
        if attempt < 5:
            print(f"      Waiting 60s before retry...")
            time.sleep(60)

    print(f"      [FAIL] Failed to suspend '{capacity_name}' after 5 attempts")
    return False
