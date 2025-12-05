#!/usr/bin/env python3
"""
Redis Cluster Demo - Tests cluster functionality with various operations
"""

from redis.cluster import RedisCluster
from redis.cluster import ClusterNode
import time
import random
import string

# Cluster connection config (using Docker internal IPs)
STARTUP_NODES = [
    ClusterNode("redis-node-1", 6379),
    ClusterNode("redis-node-2", 6379),
    ClusterNode("redis-node-3", 6379),
]
PASSWORD = "bitnami"


def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def main():
    print("=" * 60)
    print("🚀 Redis Cluster Demo")
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

    # Show cluster info
    print("\n📊 Cluster Info:")
    print("-" * 40)
    info = rc.cluster_info()
    print(f"  State: {info.get('cluster_state', 'unknown')}")
    print(f"  Slots assigned: {info.get('cluster_slots_assigned', 0)}")
    print(f"  Known nodes: {info.get('cluster_known_nodes', 0)}")

    # Demo 1: Basic SET/GET
    print("\n🔹 Demo 1: Basic SET/GET")
    print("-" * 40)
    rc.set("hello", "world")
    print(f"  SET hello = 'world'")
    value = rc.get("hello")
    print(f"  GET hello = '{value}'")

    # Demo 2: Multiple keys (distributed across nodes)
    print("\n🔹 Demo 2: Distributed Keys")
    print("-" * 40)
    keys_created = []
    for i in range(10):
        key = f"user:{i}:name"
        value = f"User_{random_string(5)}"
        rc.set(key, value)
        keys_created.append(key)
        print(f"  SET {key} = '{value}'")

    # Demo 3: Hash operations
    print("\n🔹 Demo 3: Hash Operations")
    print("-" * 40)
    rc.hset("product:1001", mapping={
        "name": "Redis Cluster Guide",
        "price": "29.99",
        "category": "Books",
        "stock": "150"
    })
    print("  HSET product:1001 with 4 fields")
    product = rc.hgetall("product:1001")
    print(f"  HGETALL product:1001 = {product}")

    # Demo 4: List operations
    print("\n🔹 Demo 4: List Operations")
    print("-" * 40)
    rc.delete("tasks:queue")
    tasks = ["send_email", "process_payment", "update_inventory", "notify_user"]
    for task in tasks:
        rc.rpush("tasks:queue", task)
    print(f"  RPUSH tasks:queue with {len(tasks)} items")
    all_tasks = rc.lrange("tasks:queue", 0, -1)
    print(f"  LRANGE tasks:queue = {all_tasks}")

    # Demo 5: Set operations
    print("\n🔹 Demo 5: Set Operations")
    print("-" * 40)
    rc.delete("tags:article:42")
    tags = ["python", "redis", "cluster", "database", "nosql"]
    rc.sadd("tags:article:42", *tags)
    print(f"  SADD tags:article:42 with {len(tags)} tags")
    stored_tags = rc.smembers("tags:article:42")
    print(f"  SMEMBERS tags:article:42 = {stored_tags}")

    # Demo 6: Sorted Set (leaderboard)
    print("\n🔹 Demo 6: Sorted Set (Leaderboard)")
    print("-" * 40)
    rc.delete("leaderboard:game1")
    players = [
        ("Alice", 1500),
        ("Bob", 2300),
        ("Charlie", 1800),
        ("Diana", 2100),
        ("Eve", 1950),
    ]
    for name, score in players:
        rc.zadd("leaderboard:game1", {name: score})
    print(f"  ZADD leaderboard:game1 with {len(players)} players")
    top3 = rc.zrevrange("leaderboard:game1", 0, 2, withscores=True)
    print("  Top 3 players:")
    for rank, (player, score) in enumerate(top3, 1):
        print(f"    {rank}. {player}: {int(score)} pts")

    # Demo 7: TTL (expiring keys)
    print("\n🔹 Demo 7: TTL (Expiring Keys)")
    print("-" * 40)
    rc.setex("session:abc123", 300, "user_42_session_data")
    ttl = rc.ttl("session:abc123")
    print(f"  SETEX session:abc123 with 300s TTL")
    print(f"  TTL session:abc123 = {ttl} seconds")

    # Demo 8: Counter with INCR
    print("\n🔹 Demo 8: Atomic Counter")
    print("-" * 40)
    rc.set("page:views:home", 0)
    for _ in range(5):
        rc.incr("page:views:home")
    views = rc.get("page:views:home")
    print(f"  INCR page:views:home x5 = {views}")

    # Show all keys created
    print("\n📋 Summary: All Keys in Cluster")
    print("-" * 40)
    
    # Scan keys from all nodes
    all_keys = []
    for key in rc.scan_iter("*", count=100):
        all_keys.append(key)
    
    print(f"  Total keys: {len(all_keys)}")
    for key in sorted(all_keys)[:20]:
        print(f"    • {key}")
    if len(all_keys) > 20:
        print(f"    ... and {len(all_keys) - 20} more")

    # Show which node each key is on
    print("\n🗺️  Key Distribution Across Nodes")
    print("-" * 40)
    sample_keys = ["hello", "user:0:name", "product:1001", "tasks:queue", "leaderboard:game1"]
    for key in sample_keys:
        slot = rc.cluster_keyslot(key)
        print(f"  {key} → slot {slot}")

    print("\n" + "=" * 60)
    print("✅ Demo completed! Check RedisInsight at http://localhost:5540")
    print("=" * 60)

    rc.close()


if __name__ == "__main__":
    main()

