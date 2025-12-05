#!/usr/bin/env python3
"""
Clear all keys from Redis Cluster
"""

from redis.cluster import RedisCluster
from redis.cluster import ClusterNode

# Cluster connection config
STARTUP_NODES = [
    ClusterNode("redis-node-1", 6379),
    ClusterNode("redis-node-2", 6379),
    ClusterNode("redis-node-3", 6379),
]
PASSWORD = "bitnami"


def clear_all_keys():
    print("=" * 60)
    print("🗑️  Clearing All Keys from Redis Cluster")
    print("=" * 60)
    
    # Connect to cluster
    print("\n📡 Connecting to Redis Cluster...")
    try:
        rc = RedisCluster(
            startup_nodes=STARTUP_NODES,
            password=PASSWORD,
            decode_responses=True
        )
        print("✅ Connected successfully!")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return
    
    # Count keys before deletion
    print("\n📊 Counting keys before deletion...")
    total_keys = 0
    keys_by_node = {}
    
    for key in rc.scan_iter("*", count=1000):
        total_keys += 1
        slot = rc.cluster_keyslot(key)
        # Determine which master based on slot
        if slot <= 5460:
            node = "Master 1"
        elif slot <= 10922:
            node = "Master 2"
        else:
            node = "Master 3"
        
        keys_by_node[node] = keys_by_node.get(node, 0) + 1
    
    print(f"  Total keys found: {total_keys}")
    for node, count in keys_by_node.items():
        print(f"    {node}: {count} keys")
    
    if total_keys == 0:
        print("\n✅ No keys to delete. Cluster is already empty!")
        rc.close()
        return
    
    # Delete all keys
    print(f"\n🗑️  Deleting {total_keys} keys...")
    deleted_count = 0
    
    for key in rc.scan_iter("*", count=1000):
        try:
            rc.delete(key)
            deleted_count += 1
            if deleted_count % 100 == 0:
                print(f"  Deleted {deleted_count} keys...")
        except Exception as e:
            print(f"  ⚠️  Error deleting {key}: {e}")
    
    print(f"\n✅ Deleted {deleted_count} keys")
    
    # Verify deletion
    print("\n🔍 Verifying deletion...")
    remaining_keys = []
    for key in rc.scan_iter("*", count=100):
        remaining_keys.append(key)
    
    if len(remaining_keys) == 0:
        print("  ✅ All keys successfully deleted!")
    else:
        print(f"  ⚠️  Warning: {len(remaining_keys)} keys still remain")
        print("  Remaining keys:")
        for key in remaining_keys[:10]:
            print(f"    • {key}")
        if len(remaining_keys) > 10:
            print(f"    ... and {len(remaining_keys) - 10} more")
    
    # Show cluster info
    print("\n📊 Cluster Info:")
    print("-" * 40)
    info = rc.cluster_info()
    print(f"  State: {info.get('cluster_state', 'unknown')}")
    print(f"  Slots assigned: {info.get('cluster_slots_assigned', 0)}")
    print(f"  Known nodes: {info.get('cluster_known_nodes', 0)}")
    
    print("\n" + "=" * 60)
    print("✅ Cluster cleared! Ready for fresh demo run.")
    print("=" * 60)
    
    rc.close()


if __name__ == "__main__":
    clear_all_keys()

