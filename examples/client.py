#!/usr/bin/env python3
"""
Python client example for DuckDB Query API.
Demonstrates how to interact with the API programmatically.
"""

import requests
import json
from typing import List, Dict, Any, Optional


class DuckDBAPIClient:
    """Client for interacting with the DuckDB Query API."""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        """
        Initialize the client.
        
        Args:
            base_url: Base URL of the API (default: localhost)
        """
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
    
    def health_check(self) -> Dict[str, Any]:
        """Check API health status."""
        response = self.session.get(f"{self.base_url}/health")
        response.raise_for_status()
        return response.json()
    
    def get_schema(self) -> Dict[str, Any]:
        """Get database schema information."""
        response = self.session.get(f"{self.base_url}/schema")
        response.raise_for_status()
        return response.json()
    
    def get_table_sample(self, table_name: str, limit: int = 5) -> Dict[str, Any]:
        """Get sample data from a table."""
        params = {"limit": limit}
        response = self.session.get(
            f"{self.base_url}/tables/{table_name}",
            params=params
        )
        response.raise_for_status()
        return response.json()
    
    def query(
        self,
        sql: str,
        params: Optional[List[Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a SQL query.
        
        Args:
            sql: SQL query with ? placeholders
            params: List of parameters to bind
            
        Returns:
            Query results
        """
        payload = {
            "sql": sql,
            "params": params or []
        }
        response = self.session.post(
            f"{self.base_url}/query",
            json=payload
        )
        response.raise_for_status()
        return response.json()
    
    def query_to_list(
        self,
        sql: str,
        params: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a query and return results as list of dicts.
        
        Args:
            sql: SQL query
            params: Parameters to bind
            
        Returns:
            List of result rows as dictionaries
        """
        result = self.query(sql, params)
        if result.get("success"):
            return result.get("data", [])
        else:
            raise Exception(f"Query failed: {result.get('error')}")


def main():
    """Run example queries."""
    
    # Initialize client
    client = DuckDBAPIClient()
    
    print("=" * 60)
    print("DuckDB Query API - Python Client Examples")
    print("=" * 60)
    
    # Example 1: Health check
    print("\n[1] Health Check")
    try:
        health = client.health_check()
        print(f"✓ Status: {health['status']}")
    except requests.exceptions.ConnectionError:
        print("✗ Could not connect to API. Is it running?")
        return
    
    # Example 2: Get schema
    print("\n[2] Database Schema")
    schema = client.get_schema()
    print(f"✓ Available tables: {list(schema['schema'].keys())}")
    
    # Example 3: Get table sample
    print("\n[3] Users Table Sample")
    users_sample = client.get_table_sample("users", limit=3)
    print(f"✓ Found {users_sample['row_count']} users (showing 3)")
    for user in users_sample["data"]:
        print(f"  - {user['username']} from {user['country']}")
    
    # Example 4: Simple SELECT
    print("\n[4] Simple SELECT Query")
    result = client.query("SELECT * FROM users LIMIT 2")
    print(f"✓ Query returned {result['row_count']} rows")
    
    # Example 5: WHERE clause with parameter
    print("\n[5] WHERE Clause (Users from USA)")
    result = client.query(
        "SELECT username, email FROM users WHERE country = ?",
        params=["USA"]
    )
    print(f"✓ Found {result['row_count']} USA users:")
    for user in result["data"]:
        print(f"  - {user['username']} ({user['email']})")
    
    # Example 6: GROUP BY with aggregation
    print("\n[6] GROUP BY - Users per Country")
    result = client.query(
        "SELECT country, COUNT(*) as user_count FROM users GROUP BY country ORDER BY user_count DESC"
    )
    print(f"✓ User distribution:")
    for row in result["data"]:
        print(f"  - {row['country']}: {row['user_count']} users")
    
    # Example 7: JOIN query
    print("\n[7] INNER JOIN - User Orders")
    result = client.query(
        "SELECT u.username, o.product_name, o.amount FROM users u "
        "INNER JOIN orders o ON u.user_id = o.user_id "
        "WHERE u.country = ? LIMIT 5",
        params=["USA"]
    )
    print(f"✓ Found {result['row_count']} orders from USA users:")
    for order in result["data"]:
        print(f"  - {order['username']}: {order['product_name']} (${order['amount']})")
    
    # Example 8: Aggregation functions
    print("\n[8] Aggregations - Order Statistics")
    result = client.query(
        "SELECT COUNT(*) as total_orders, SUM(amount) as total_revenue, "
        "AVG(amount) as avg_order, MIN(amount) as min_order, MAX(amount) as max_order "
        "FROM orders"
    )
    stats = result["data"][0]
    print(f"✓ Order Statistics:")
    print(f"  - Total Orders: {stats['total_orders']}")
    print(f"  - Total Revenue: ${stats['total_revenue']:.2f}")
    print(f"  - Avg Order: ${stats['avg_order']:.2f}")
    print(f"  - Min: ${stats['min_order']:.2f}")
    print(f"  - Max: ${stats['max_order']:.2f}")
    
    # Example 9: Multiple parameters
    print("\n[9] Multiple Parameters - Age & Country Filter")
    result = client.query(
        "SELECT username, age, country FROM users "
        "WHERE country = ? AND age > ? "
        "ORDER BY age DESC",
        params=["USA", 30]
    )
    print(f"✓ Found {result['row_count']} USA users older than 30:")
    for user in result["data"]:
        print(f"  - {user['username']} ({user['age']} years old)")
    
    # Example 10: Using client helper method
    print("\n[10] Using query_to_list Helper")
    orders = client.query_to_list(
        "SELECT * FROM orders WHERE status = ?",
        params=["completed"]
    )
    print(f"✓ Found {len(orders)} completed orders")
    print(f"  - Total value: ${sum(o['amount'] for o in orders):.2f}")
    
    print("\n" + "=" * 60)
    print("All examples completed successfully! ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
