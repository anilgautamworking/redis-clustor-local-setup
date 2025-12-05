#!/usr/bin/env python3
"""
Verify Hash Tags - Shows which node each category is stored on
"""

from redis.cluster import RedisCluster
from redis.cluster import ClusterNode

STARTUP_NODES = [
    ClusterNode("redis-node-1", 6379),
    ClusterNode("redis-node-2", 6379),
    ClusterNode("redis-node-3", 6379),
]
PASSWORD = "bitnami"


def get_master_for_slot(slot):
    """Determine which master node owns a slot"""
    if slot <= 5460:
        return "Master 1 (redis-node-1)", "172.28.0.2"
    elif slot <= 10922:
        return "Master 2 (redis-node-2)", "172.28.0.3"
    else:
        return "Master 3 (redis-node-3)", "172.28.0.4"


def main():
    print("=" * 80)
    print("🔍 VERIFYING HASH TAGS - Where Each Category is Stored")
    print("=" * 80)
    
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
    
    # Categories to check
    categories = ["electronics", "books", "clothing"]
    
    print("\n" + "=" * 80)
    print("📊 CATEGORY DISTRIBUTION ACROSS NODES")
    print("=" * 80)
    
    category_locations = {}
    
    for category in categories:
        print(f"\n📁 Category: {category}")
        print("-" * 80)
        
        # Check all keys for this category
        category_key = f"{{category:{category}}}"
        products_key = f"{{category:{category}}}:products"
        
        # Get slots
        category_slot = rc.cluster_keyslot(category_key)
        products_slot = rc.cluster_keyslot(products_key)
        
        # Get master info
        category_master, category_ip = get_master_for_slot(category_slot)
        products_master, products_ip = get_master_for_slot(products_slot)
        
        print(f"  Category key: {category_key}")
        print(f"    Slot: {category_slot}")
        print(f"    Master: {category_master}")
        print(f"    IP: {category_ip}")
        
        print(f"\n  Products list: {products_key}")
        print(f"    Slot: {products_slot}")
        print(f"    Master: {products_master}")
        print(f"    IP: {products_ip}")
        
        # Check if same slot
        same_slot = category_slot == products_slot
        print(f"\n  ✅ Same slot: {same_slot}")
        
        if same_slot:
            category_locations[category] = {
                "slot": category_slot,
                "master": category_master,
                "ip": category_ip
            }
        
        # Check product keys
        print(f"\n  Product keys:")
        product_keys = []
        for key in rc.scan_iter(f"{{category:{category}}}:product:*", count=100):
            product_keys.append(key)
        
        if product_keys:
            for pk in sorted(product_keys):
                pk_slot = rc.cluster_keyslot(pk)
                pk_master, pk_ip = get_master_for_slot(pk_slot)
                same_as_category = pk_slot == category_slot
                status = "✅ Same" if same_as_category else "❌ Different"
                print(f"    {pk}")
                print(f"      Slot: {pk_slot} | Master: {pk_master} | {status}")
        else:
            print("    (No product keys found)")
    
    # Summary
    print("\n" + "=" * 80)
    print("📋 SUMMARY - Category Locations")
    print("=" * 80)
    
    print("\n  Each category is stored on ONE master node:")
    print("-" * 80)
    
    for category, info in category_locations.items():
        print(f"  {category.upper():15} → Slot {info['slot']:5} → {info['master']}")
    
    print("\n  ⚠️  IMPORTANT:")
    print("    - Each category is on a DIFFERENT node (by design)")
    print("    - All keys WITHIN a category are on the SAME node")
    print("    - This is why you see mixed categories when viewing one node")
    
    print("\n" + "=" * 80)
    print("🔍 WHY YOU SEE MIXED CATEGORIES")
    print("=" * 80)
    
    print("""
  When you view redis-node-3 in RedisInsight, you're seeing:
  
  ✅ All keys stored on Master 3 (redis-node-3)
  ❌ NOT all keys from the cluster
  
  Example:
    - If 'electronics' is on Master 1 → You WON'T see it on node-3
    - If 'books' is on Master 2 → You WON'T see it on node-3
    - If 'clothing' is on Master 3 → You WILL see it on node-3
  
  To see ALL categories, you need to:
  1. Add all 3 master nodes to RedisInsight
  2. Or use the Python client (which queries all nodes)
    """)
    
    # Show which keys are on which node
    print("\n" + "=" * 80)
    print("🗺️  KEY DISTRIBUTION BY NODE")
    print("=" * 80)
    
    nodes = {
        "Master 1": [],
        "Master 2": [],
        "Master 3": []
    }
    
    # Scan all keys
    all_keys = []
    for key in rc.scan_iter("*", count=1000):
        all_keys.append(key)
    
    for key in all_keys:
        slot = rc.cluster_keyslot(key)
        master_name, _ = get_master_for_slot(slot)
        # Extract just "Master 1", "Master 2", or "Master 3"
        master_key = master_name.split()[0] + " " + master_name.split()[1]
        if master_key in nodes:
            nodes[master_key].append(key)
    
    for master_name, keys in nodes.items():
        print(f"\n  {master_name}:")
        print(f"    Total keys: {len(keys)}")
        if keys:
            print("    Keys:")
            for key in sorted(keys)[:10]:
                print(f"      • {key}")
            if len(keys) > 10:
                print(f"      ... and {len(keys) - 10} more")
    
    print("\n" + "=" * 80)
    print("✅ Verification complete!")
    print("=" * 80)
    
    rc.close()


if __name__ == "__main__":
    main()

