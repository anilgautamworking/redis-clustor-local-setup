#!/usr/bin/env python3
"""
Real-World E-commerce Example Using Redis Cluster
Demonstrates how a website would actually use Redis Cluster
"""

from redis.cluster import RedisCluster
from redis.cluster import ClusterNode
import json
import time
from datetime import datetime

# Cluster connection
STARTUP_NODES = [
    ClusterNode("redis-node-1", 6379),
    ClusterNode("redis-node-2", 6379),
    ClusterNode("redis-node-3", 6379),
]
PASSWORD = "bitnami"


class EcommerceRedis:
    """E-commerce website using Redis Cluster"""
    
    def __init__(self):
        self.rc = RedisCluster(
            startup_nodes=STARTUP_NODES,
            password=PASSWORD,
            decode_responses=True
        )
    
    # ========== PRODUCT CATALOG ==========
    def add_product(self, product_id, name, price, stock, category):
        """Add product to catalog"""
        self.rc.hset(f"product:{product_id}", mapping={
            "name": name,
            "price": str(price),
            "stock": str(stock),
            "category": category,
            "views": "0",
            "created_at": datetime.now().isoformat()
        })
        print(f"✅ Added product {product_id}: {name}")
    
    def get_product(self, product_id):
        """Get product details (fast lookup, no DB query)"""
        return self.rc.hgetall(f"product:{product_id}")
    
    def increment_product_views(self, product_id):
        """Track product page views"""
        self.rc.hincrby(f"product:{product_id}", "views", 1)
    
    # ========== SHOPPING CART ==========
    def add_to_cart(self, user_id, product_id, quantity):
        """Add item to user's shopping cart"""
        cart_key = f"cart:{user_id}"
        self.rc.hset(cart_key, product_id, str(quantity))
        # Set cart expiry to 7 days
        self.rc.expire(cart_key, 7 * 24 * 3600)
        print(f"✅ Added {quantity}x product {product_id} to cart for user {user_id}")
    
    def get_cart(self, user_id):
        """Get user's shopping cart"""
        return self.rc.hgetall(f"cart:{user_id}")
    
    def remove_from_cart(self, user_id, product_id):
        """Remove item from cart"""
        self.rc.hdel(f"cart:{user_id}", product_id)
    
    # ========== USER SESSIONS ==========
    def create_session(self, user_id, email, role="customer"):
        """Create user session (expires in 1 hour)"""
        session_id = f"sess_{user_id}_{int(time.time())}"
        session_data = json.dumps({
            "user_id": user_id,
            "email": email,
            "role": role,
            "login_time": datetime.now().isoformat()
        })
        self.rc.setex(f"session:{session_id}", 3600, session_data)
        print(f"✅ Created session {session_id} for user {user_id}")
        return session_id
    
    def get_session(self, session_id):
        """Get session data"""
        session = self.rc.get(f"session:{session_id}")
        return json.loads(session) if session else None
    
    def extend_session(self, session_id, seconds=3600):
        """Extend session expiry"""
        self.rc.expire(f"session:{session_id}", seconds)
    
    # ========== RATE LIMITING ==========
    def check_rate_limit(self, ip_address, max_requests=100, window=60):
        """Check if IP exceeded rate limit"""
        key = f"rate_limit:{ip_address}"
        current = self.rc.incr(key)
        
        if current == 1:
            self.rc.expire(key, window)
        
        if current > max_requests:
            return False, f"Rate limit exceeded: {current}/{max_requests}"
        
        return True, f"OK: {current}/{max_requests}"
    
    # ========== RECOMMENDATIONS / TRENDING ==========
    def track_product_view(self, user_id, product_id):
        """Track what products user viewed (for recommendations)"""
        self.rc.lpush(f"views:{user_id}", product_id)
        self.rc.ltrim(f"views:{user_id}", 0, 49)  # Keep last 50 views
    
    def get_recently_viewed(self, user_id, count=10):
        """Get user's recently viewed products"""
        return self.rc.lrange(f"views:{user_id}", 0, count - 1)
    
    def add_to_trending(self, product_id):
        """Add product to trending list"""
        self.rc.zadd("trending:products", {product_id: time.time()})
        # Keep only last 100 trending products
        self.rc.zremrangebyrank("trending:products", 0, -101)
    
    def get_trending_products(self, count=10):
        """Get trending products"""
        return self.rc.zrevrange("trending:products", 0, count - 1)
    
    # ========== INVENTORY MANAGEMENT ==========
    def check_stock(self, product_id):
        """Check product stock"""
        stock = self.rc.hget(f"product:{product_id}", "stock")
        return int(stock) if stock else 0
    
    def reserve_stock(self, product_id, quantity):
        """Reserve stock (atomic operation)"""
        # Use distributed lock to prevent overselling
        lock_key = f"lock:stock:{product_id}"
        if self.rc.set(lock_key, "locked", nx=True, ex=5):
            try:
                current_stock = self.check_stock(product_id)
                if current_stock >= quantity:
                    self.rc.hincrby(f"product:{product_id}", "stock", -quantity)
                    return True
                return False
            finally:
                self.rc.delete(lock_key)
        return False
    
    # ========== NOTIFICATIONS ==========
    def add_notification(self, user_id, notification_type, message):
        """Add notification to user's queue"""
        notification = json.dumps({
            "type": notification_type,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "read": False
        })
        self.rc.lpush(f"notifications:{user_id}", notification)
        self.rc.ltrim(f"notifications:{user_id}", 0, 99)  # Keep last 100
    
    def get_notifications(self, user_id, count=10):
        """Get user's recent notifications"""
        return [json.loads(n) for n in self.rc.lrange(f"notifications:{user_id}", 0, count - 1)]
    
    # ========== ANALYTICS ==========
    def track_event(self, event_type, user_id=None, product_id=None):
        """Track analytics events"""
        event = {
            "type": event_type,
            "user_id": user_id,
            "product_id": product_id,
            "timestamp": time.time()
        }
        # Add to analytics queue
        self.rc.lpush("analytics:events", json.dumps(event))
        self.rc.ltrim("analytics:events", 0, 9999)  # Keep last 10k events
    
    def get_daily_stats(self, date):
        """Get daily statistics"""
        key = f"stats:{date}"
        return {
            "page_views": int(self.rc.get(f"{key}:page_views") or 0),
            "orders": int(self.rc.get(f"{key}:orders") or 0),
            "revenue": float(self.rc.get(f"{key}:revenue") or 0)
        }
    
    def increment_stat(self, date, stat_name, value=1):
        """Increment daily statistic"""
        self.rc.incrby(f"stats:{date}:{stat_name}", value)


def demo_ecommerce_workflow():
    """Simulate real e-commerce website workflow"""
    print("=" * 70)
    print("🛒 E-COMMERCE WEBSITE - REDIS CLUSTER DEMO")
    print("=" * 70)
    
    store = EcommerceRedis()
    
    # 1. Add products to catalog
    print("\n📦 STEP 1: Adding Products to Catalog")
    print("-" * 70)
    store.add_product("P001", "iPhone 15 Pro", 999.00, 50, "Electronics")
    store.add_product("P002", "MacBook Pro", 1999.00, 30, "Electronics")
    store.add_product("P003", "AirPods Pro", 249.00, 100, "Electronics")
    
    # 2. User logs in
    print("\n👤 STEP 2: User Login & Session Creation")
    print("-" * 70)
    user_id = "U12345"
    session_id = store.create_session(user_id, "john@example.com", "premium")
    session = store.get_session(session_id)
    print(f"   Session data: {session}")
    
    # 3. User browses products
    print("\n🔍 STEP 3: User Browsing Products")
    print("-" * 70)
    store.track_product_view(user_id, "P001")
    store.track_product_view(user_id, "P002")
    store.increment_product_views("P001")
    store.add_to_trending("P001")
    print(f"   Recently viewed: {store.get_recently_viewed(user_id)}")
    
    # 4. Add to cart
    print("\n🛒 STEP 4: Shopping Cart Operations")
    print("-" * 70)
    store.add_to_cart(user_id, "P001", 2)
    store.add_to_cart(user_id, "P003", 1)
    cart = store.get_cart(user_id)
    print(f"   Cart contents: {cart}")
    
    # 5. Check stock & reserve
    print("\n📊 STEP 5: Inventory Management")
    print("-" * 70)
    stock = store.check_stock("P001")
    print(f"   Product P001 stock: {stock}")
    reserved = store.reserve_stock("P001", 2)
    print(f"   Reserved 2 units: {reserved}")
    new_stock = store.check_stock("P001")
    print(f"   New stock: {new_stock}")
    
    # 6. Rate limiting
    print("\n🚦 STEP 6: Rate Limiting")
    print("-" * 70)
    ip = "192.168.1.100"
    for i in range(3):
        allowed, msg = store.check_rate_limit(ip, max_requests=100)
        print(f"   Request {i+1}: {msg}")
    
    # 7. Notifications
    print("\n🔔 STEP 7: User Notifications")
    print("-" * 70)
    store.add_notification(user_id, "order", "Your order #12345 has been shipped!")
    store.add_notification(user_id, "promotion", "20% off on all Electronics!")
    notifications = store.get_notifications(user_id)
    for notif in notifications:
        print(f"   [{notif['type']}] {notif['message']}")
    
    # 8. Analytics
    print("\n📈 STEP 8: Analytics Tracking")
    print("-" * 70)
    store.track_event("page_view", user_id, "P001")
    store.track_event("add_to_cart", user_id, "P001")
    store.increment_stat("2024-12-05", "page_views", 1)
    stats = store.get_daily_stats("2024-12-05")
    print(f"   Daily stats: {stats}")
    
    # 9. Trending products
    print("\n🔥 STEP 9: Trending Products")
    print("-" * 70)
    trending = store.get_trending_products(5)
    print(f"   Trending: {trending}")
    
    print("\n" + "=" * 70)
    print("✅ E-commerce workflow completed!")
    print("=" * 70)


if __name__ == "__main__":
    demo_ecommerce_workflow()

