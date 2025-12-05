#!/usr/bin/env python3
"""
Redis Cluster Data Distribution Demo
Shows how data is SHARDED (not replicated) across nodes
"""

from redis.cluster import RedisCluster
from redis.cluster import ClusterNode
import json

STARTUP_NODES = [
    ClusterNode("redis-node-1", 6379),
    ClusterNode("redis-node-2", 6379),
    ClusterNode("redis-node-3", 6379),
]
PASSWORD = "bitnami"


def get_node_for_key(rc, key):
    """Find which node a key belongs to"""
    slot = rc.cluster_keyslot(key)
    
    # Get cluster nodes info
    nodes = rc.cluster_nodes()
    
    # Find which master owns this slot
    for node_id, node_info in nodes.items():
        if node_info.get('master') is None:  # It's a master
            slots = node_info.get('slots', [])
            if slots:
                start, end = slots[0]
                if start <= slot <= end:
                    return node_info.get('host'), node_info.get('port'), slot
    
    return None, None, slot


def demo_distribution():
    print("=" * 80)
    print("🔍 REDIS CLUSTER DATA DISTRIBUTION EXPLAINED")
    print("=" * 80)
    
    rc = RedisCluster(
        startup_nodes=STARTUP_NODES,
        password=PASSWORD,
        decode_responses=True
    )
    
    # Get cluster info
    info = rc.cluster_info()
    print(f"\n📊 Cluster Status: {info.get('cluster_state')}")
    print(f"   Total Slots: {info.get('cluster_slots_assigned')}/16384")
    print(f"   Known Nodes: {info.get('cluster_known_nodes')}")
    
    # Show slot distribution
    print("\n" + "=" * 80)
    print("📦 SLOT DISTRIBUTION ACROSS 3 MASTERS")
    print("=" * 80)
    print("""
    Master 1 (redis-node-1): Slots 0     - 5460   (5,461 slots)
    Master 2 (redis-node-2): Slots 5461  - 10922  (5,462 slots)
    Master 3 (redis-node-3): Slots 10923 - 16383  (5,461 slots)
    
    Total: 16,384 slots (distributed evenly)
    """)
    
    # Create products
    print("\n" + "=" * 80)
    print("🛍️  CREATING 10 PRODUCTS")
    print("=" * 80)
    
    products = {}
    for i in range(1, 11):
        product_id = f"product:{i:05d}"
        rc.set(product_id, f"Product {i} Data")
        
        # Calculate which slot this key belongs to
        slot = rc.cluster_keyslot(product_id)
        
        # Determine which master based on slot
        if slot <= 5460:
            master = "Master 1 (redis-node-1)"
        elif slot <= 10922:
            master = "Master 2 (redis-node-2)"
        else:
            master = "Master 3 (redis-node-3)"
        
        products[product_id] = {
            "slot": slot,
            "master": master
        }
        
        print(f"  {product_id:15} → Slot {slot:5} → {master}")
    
    # Group by master
    print("\n" + "=" * 80)
    print("📊 PRODUCTS GROUPED BY MASTER NODE")
    print("=" * 80)
    
    master1_products = [p for p, info in products.items() if "Master 1" in info["master"]]
    master2_products = [p for p, info in products.items() if "Master 2" in info["master"]]
    master3_products = [p for p, info in products.items() if "Master 3" in info["master"]]
    
    print(f"\n  Master 1 (redis-node-1): {len(master1_products)} products")
    for p in master1_products:
        print(f"    • {p} (slot {products[p]['slot']})")
    
    print(f"\n  Master 2 (redis-node-2): {len(master2_products)} products")
    for p in master2_products:
        print(f"    • {p} (slot {products[p]['slot']})")
    
    print(f"\n  Master 3 (redis-node-3): {len(master3_products)} products")
    for p in master3_products:
        print(f"    • {p} (slot {products[p]['slot']})")
    
    # Demonstrate lookup
    print("\n" + "=" * 80)
    print("🔍 HOW REDIS FINDS A PRODUCT (e.g., product:00123)")
    print("=" * 80)
    
    lookup_key = "product:00123"
    slot = rc.cluster_keyslot(lookup_key)
    
    print(f"""
    Step 1: Client wants to GET "{lookup_key}"
    
    Step 2: Redis calculates hash slot:
            slot = CRC16("{lookup_key}") % 16384
            slot = {slot}
    
    Step 3: Redis knows slot {slot} belongs to:
            {"Master 1 (redis-node-1)" if slot <= 5460 else "Master 2 (redis-node-2)" if slot <= 10922 else "Master 3 (redis-node-3)"}
    
    Step 4: Client automatically routes request to correct node
    
    Step 5: Node returns data (or "nil" if not found)
    """)
    
    # Test the lookup
    value = rc.get(lookup_key)
    if value:
        print(f"    ✅ Found: {value}")
    else:
        print(f"    ❌ Not found (we didn't create product:00123)")
    
    # Show what happens when you query a product that exists
    if master1_products:
        test_key = master1_products[0]
        print(f"\n    Testing lookup of {test_key}:")
        value = rc.get(test_key)
        print(f"    ✅ Found on Master 1: {value}")
    
    # Explain replication
    print("\n" + "=" * 80)
    print("🔄 REPLICATION (NOT SHARING)")
    print("=" * 80)
    print("""
    ❌ WRONG ASSUMPTION:
       "All products exist on all nodes"
    
    ✅ CORRECT:
       - Each product exists on ONLY ONE master
       - Each master has a REPLICA (backup copy)
       - Replica only has copy of ITS master's data, not all data
    
    Example:
       Master 1 (redis-node-1) has: product:00001, product:00005
       Replica 1 (redis-node-4) has: COPY of product:00001, product:00005
       
       Master 2 (redis-node-2) has: product:00002, product:00007
       Replica 2 (redis-node-5) has: COPY of product:00002, product:00007
       
       Master 3 (redis-node-3) has: product:00003, product:00009
       Replica 3 (redis-node-6) has: COPY of product:00003, product:00009
    
    If Master 1 crashes → Replica 1 takes over → Only products 00001, 00005 available
    Products 00002, 00003, etc. are still on their respective masters
    """)
    
    # Show hash function
    print("\n" + "=" * 80)
    print("🔢 HOW HASH SLOT IS CALCULATED")
    print("=" * 80)
    print("""
    Redis uses CRC16 algorithm:
    
    1. Takes the key: "product:00123"
    2. Calculates CRC16 hash: 12345 (example)
    3. Modulo 16384: 12345 % 16384 = 12345
    4. Slot = 12345 → Belongs to Master 3 (slots 10923-16383)
    
    Same key ALWAYS maps to same slot → Same node
    This is why you can't use multi-key operations across different nodes!
    """)
    
    # Demonstrate that same key always goes to same node
    print("\n" + "=" * 80)
    print("✅ CONSISTENCY TEST: Same key → Same slot → Same node")
    print("=" * 80)
    
    test_key = "product:00123"
    slot1 = rc.cluster_keyslot(test_key)
    slot2 = rc.cluster_keyslot(test_key)
    slot3 = rc.cluster_keyslot(test_key)
    
    print(f"    Key: {test_key}")
    print(f"    Slot (call 1): {slot1}")
    print(f"    Slot (call 2): {slot2}")
    print(f"    Slot (call 3): {slot3}")
    print(f"    ✅ Always same: {slot1 == slot2 == slot3}")
    
    print("\n" + "=" * 80)
    print("💡 KEY TAKEAWAYS")
    print("=" * 80)
    print("""
    1. Data is SHARDED (split), not replicated across all nodes
    2. Each key exists on ONLY ONE master node
    3. Redis uses hash slots (0-16383) to determine which node
    4. Client library automatically routes requests to correct node
    5. Replicas only backup their master's data, not all data
    6. Same key ALWAYS goes to same node (consistent hashing)
    """)
    
    rc.close()


if __name__ == "__main__":
    demo_distribution()

