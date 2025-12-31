"""Capacity management module for Azure Fabric capacities."""

import time
from .utils import call_azure_api


def capacity_exists(capacity_name, subscription_id, resource_group):
    """Check if a Fabric capacity exists in Azure."""
    status, _ = call_azure_api(
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version=2023-11-01")
    return status == 200


def create_capacity(capacity_config, subscription_id, resource_group, defaults):
    """
    Create a Fabric capacity in Azure.

    Args:
        capacity_config: Dict with capacity configuration (name, region, sku, admin_members)
        subscription_id: Azure subscription ID
        resource_group: Azure resource group name
        defaults: Dict with default values for capacity settings

    Returns:
        None
    """
    capacity_name = capacity_config['name']

    if capacity_exists(capacity_name, subscription_id, resource_group):
        print(f"✓ {capacity_name} exists")
        call_azure_api(
            f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/resume?api-version=2023-11-01", 'post')
        return

    admin_members = capacity_config.get(
        'admin_members', defaults.get('capacity_admins', ''))
    admin_members = admin_members if isinstance(admin_members, list) else [
        admin_id.strip() for admin_id in admin_members.split(',') if admin_id.strip()]

    request_body = {
        "location": capacity_config.get('region', defaults.get('region')),
        "sku": {"name": capacity_config.get('sku', defaults.get('sku')), "tier": "Fabric"},
        "properties": {"administration": {"members": admin_members}}
    }

    status, response = call_azure_api(
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version=2023-11-01", 'put', request_body)

    if status in [200, 201]:
        print(f"✓ Created {capacity_name}")
        time.sleep(40)


def suspend_capacity(capacity_name, subscription_id, resource_group):
    """
    Suspend a Fabric capacity to stop billing.

    Args:
        capacity_name: Name of the capacity to suspend
        subscription_id: Azure subscription ID
        resource_group: Azure resource group name

    Returns:
        bool: True if suspended successfully, False otherwise
    """
    for _ in range(5):
        status, _ = call_azure_api(
            f"subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/suspend?api-version=2023-11-01", 'post')
        if status in [200, 202]:
            print(f"✓ Suspended {capacity_name}")
            return True
        time.sleep(60)
    print(f"✗ Failed to suspend {capacity_name}")
    return False

def resume_capacity(capacity_name, subscription_id, resource_group):
    """
    Resume a suspended Fabric capacity.
    
    Args:
        capacity_name: Name of the capacity to resume
        subscription_id: Azure subscription ID
        resource_group: Azure resource group name
    
    Returns:
        bool: True if resume initiated successfully
    """
    print(f"▶️  Resuming capacity {capacity_name}...", flush=True)
    
    status, response = call_azure_api(
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/resume?api-version=2023-11-01",
        'post'
    )
    
    if status in [200, 202]:
        print(f"✓ Resume initiated for {capacity_name}", flush=True)
        return True
    else:
        print(f"✗ Failed to resume {capacity_name}: {response}", flush=True)
        return False


def wait_for_capacity_active(capacity_name, subscription_id, resource_group, timeout=300):
    """
    Wait for capacity to be in Active state.
    
    Args:
        capacity_name: Name of the capacity
        subscription_id: Azure subscription ID
        resource_group: Azure resource group name
        timeout: Maximum seconds to wait (default 300 = 5 minutes)
    
    Returns:
        bool: True if capacity became active, False if timeout
    """
    import time
    
    print(f"⏳ Waiting for capacity {capacity_name} to be Active...", flush=True)
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        status, response = call_azure_api(
            f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version=2023-11-01"
        )
        
        if status == 200:
            state = response.get('properties', {}).get('state', '')
            provisioning_state = response.get('properties', {}).get('provisioningState', '')
            
            print(f"  State: {state}, Provisioning: {provisioning_state}", flush=True)
            
            if state == 'Active' and provisioning_state == 'Succeeded':
                print(f"✓ Capacity {capacity_name} is Active and ready", flush=True)
                return True
        
        time.sleep(20)  # Check every 20 seconds
    
    print(f"✗ Timeout waiting for capacity {capacity_name} after {timeout}s", flush=True)
    return False