#!/usr/bin/env python3
"""
Category and Products Demo - Using Hash Tags {} to group related data
Shows how products in the same category are stored on the same node
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


def main():
    print("=" * 70)
    print("🛍️  CATEGORY & PRODUCTS DEMO - Using Hash Tags {{}}")
    print("=" * 70)
    
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
    
    # Show cluster info
    print("\n📊 Cluster Info:")
    print("-" * 70)
    info = rc.cluster_info()
    print(f"  State: {info.get('cluster_state', 'unknown')}")
    print(f"  Slots assigned: {info.get('cluster_slots_assigned', 0)}")
    
    # ========== DEMO: Categories and Products ==========
    print("\n" + "=" * 70)
    print("📦 CREATING CATEGORIES AND PRODUCTS")
    print("=" * 70)
    
    # Define categories and products
    categories = {
        "electronics": [
            {"id": "P001", "name": "iPhone 15 Pro", "price": 999.00, "stock": 50},
            {"id": "P002", "name": "MacBook Pro", "price": 1999.00, "stock": 30},
            {"id": "P003", "name": "AirPods Pro", "price": 249.00, "stock": 100},
            {"id": "P004", "name": "iPad Air", "price": 599.00, "stock": 75},
        ],
        "books": [
            {"id": "P101", "name": "Redis Cluster Guide", "price": 29.99, "stock": 150},
            {"id": "P102", "name": "Python Mastery", "price": 39.99, "stock": 200},
            {"id": "P103", "name": "System Design", "price": 49.99, "stock": 120},
        ],
        "clothing": [
            {"id": "P201", "name": "Cotton T-Shirt", "price": 19.99, "stock": 500},
            {"id": "P202", "name": "Jeans", "price": 79.99, "stock": 300},
            {"id": "P203", "name": "Sneakers", "price": 129.99, "stock": 250},
        ]
    }
    
    # Store categories and products using hash tags
    print("\n🔹 Storing Categories with Hash Tags {{category:name}}")
    print("-" * 70)
    
    category_slots = {}
    
    for category_name, products in categories.items():
        # Store category info
        category_key = f"{{category:{category_name}}}"
        rc.hset(category_key, mapping={
            "name": category_name.title(),
            "product_count": str(len(products)),
            "description": f"All {category_name} products"
        })
        
        # Store category slot
        slot = rc.cluster_keyslot(category_key)
        category_slots[category_name] = slot
        
        print(f"  ✅ Category: {category_name}")
        print(f"     Key: {category_key}")
        print(f"     Slot: {slot}")
        print(f"     Products: {len(products)}")
        
        # Store products in this category using same hash tag
        print(f"\n     Products in {category_name}:")
        for product in products:
            product_key = f"{{category:{category_name}}}:product:{product['id']}"
            
            # Store product details
            rc.hset(product_key, mapping={
                "id": product['id'],
                "name": product['name'],
                "price": str(product['price']),
                "stock": str(product['stock']),
                "category": category_name
            })
            
            # Verify product is on same slot as category
            product_slot = rc.cluster_keyslot(product_key)
            
            print(f"       • {product['id']}: {product['name']}")
            print(f"         Key: {product_key}")
            print(f"         Slot: {product_slot} {'✅ Same as category' if product_slot == slot else '❌ Different!'}")
        
        # Store product IDs list for this category
        product_ids = [p['id'] for p in products]
        product_list_key = f"{{category:{category_name}}}:products"
        rc.set(product_list_key, json.dumps(product_ids))
        
        list_slot = rc.cluster_keyslot(product_list_key)
        print(f"\n     Product list key: {product_list_key}")
        print(f"     Slot: {list_slot} {'✅ Same as category' if list_slot == slot else '❌ Different!'}")
        print()
    
    # ========== VERIFY: All category data on same node ==========
    print("\n" + "=" * 70)
    print("✅ VERIFICATION: All Category Data on Same Node")
    print("=" * 70)
    
    for category_name in categories.keys():
        print(f"\n📁 Category: {category_name}")
        print("-" * 70)
        
        category_key = f"{{category:{category_name}}}"
        products_key = f"{{category:{category_name}}}:products"
        
        # Get all keys for this category
        category_slot = rc.cluster_keyslot(category_key)
        products_slot = rc.cluster_keyslot(products_key)
        
        # Get product keys
        product_keys = []
        for product in categories[category_name]:
            product_key = f"{{category:{category_name}}}:product:{product['id']}"
            product_keys.append(product_key)
        
        # Check all slots
        all_slots = [category_slot, products_slot]
        for pk in product_keys:
            all_slots.append(rc.cluster_keyslot(pk))
        
        # Verify all same
        all_same = len(set(all_slots)) == 1
        
        print(f"  Category key slot: {category_slot}")
        print(f"  Products list slot: {products_slot}")
        for pk in product_keys:
            print(f"  {pk.split(':')[-1]} slot: {rc.cluster_keyslot(pk)}")
        
        print(f"\n  ✅ All keys on same slot: {all_same}")
        print(f"  📍 Slot number: {category_slot}")
        
        # Determine which master
        if category_slot <= 5460:
            master = "Master 1 (redis-node-1)"
        elif category_slot <= 10922:
            master = "Master 2 (redis-node-2)"
        else:
            master = "Master 3 (redis-node-3)"
        
        print(f"  🖥️  Stored on: {master}")
    
    # ========== DEMO: Querying products by category ==========
    print("\n" + "=" * 70)
    print("🔍 QUERYING PRODUCTS BY CATEGORY")
    print("=" * 70)
    
    category_to_query = "electronics"
    print(f"\n📦 Getting all products in '{category_to_query}' category:")
    print("-" * 70)
    
    # Get category info
    category_key = f"{{category:{category_to_query}}}"
    category_info = rc.hgetall(category_key)
    print(f"  Category Info: {category_info}")
    
    # Get product list
    products_list_key = f"{{category:{category_to_query}}}:products"
    product_ids = json.loads(rc.get(products_list_key))
    print(f"  Product IDs: {product_ids}")
    
    # Get each product
    print(f"\n  Product Details:")
    for product_id in product_ids:
        product_key = f"{{category:{category_to_query}}}:product:{product_id}"
        product = rc.hgetall(product_key)
        print(f"    {product_id}: {product['name']} - ${product['price']} (Stock: {product['stock']})")
    
    # ========== DEMO: Without hash tags (comparison) ==========
    print("\n" + "=" * 70)
    print("⚠️  COMPARISON: Without Hash Tags")
    print("=" * 70)
    
    print("\n  Creating products WITHOUT hash tags:")
    print("-" * 70)
    
    # Create products without hash tags
    rc.set("product:NO-TAG-001", "Product without tag")
    rc.set("product:NO-TAG-002", "Product without tag")
    rc.set("product:NO-TAG-003", "Product without tag")
    
    slots_no_tag = []
    for i in range(1, 4):
        key = f"product:NO-TAG-00{i}"
        slot = rc.cluster_keyslot(key)
        slots_no_tag.append(slot)
        print(f"    {key} → Slot {slot}")
    
    all_same_no_tag = len(set(slots_no_tag)) == 1
    print(f"\n  ✅ All on same slot: {all_same_no_tag}")
    print(f"  ❌ Products distributed across different nodes!")
    
    # ========== SUMMARY ==========
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    
    print("\n  Hash Tags {{}} Benefits:")
    print("    ✅ All category data on same node")
    print("    ✅ All products in category on same node")
    print("    ✅ Can use multi-key operations (MGET, MSET)")
    print("    ✅ Better data locality")
    print("    ✅ Faster queries (single node)")
    
    print("\n  Without Hash Tags:")
    print("    ❌ Data distributed randomly")
    print("    ❌ Related data on different nodes")
    print("    ❌ Cannot use multi-key operations")
    print("    ❌ Slower queries (multiple nodes)")
    
    # Show all keys created
    print("\n" + "=" * 70)
    print("📋 All Keys Created")
    print("=" * 70)
    
    all_keys = []
    for key in rc.scan_iter("*", count=1000):
        all_keys.append(key)
    
    print(f"\n  Total keys: {len(all_keys)}")
    
    # Group by category
    for category_name in categories.keys():
        category_keys = [k for k in all_keys if f"{{category:{category_name}}}" in k]
        print(f"\n  {category_name.upper()} category ({len(category_keys)} keys):")
        for key in sorted(category_keys)[:5]:
            print(f"    • {key}")
        if len(category_keys) > 5:
            print(f"    ... and {len(category_keys) - 5} more")
    
    print("\n" + "=" * 70)
    print("✅ Demo completed! Check RedisInsight at http://localhost:5540")
    print("=" * 70)
    
    rc.close()


if __name__ == "__main__":
    main()

