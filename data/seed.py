"""
Dummy data generation for demo purposes.
Creates users and orders DataFrames for DuckDB queries.
"""

import pandas as pd
from datetime import datetime, timedelta


def create_dummy_users() -> pd.DataFrame:
    """
    Create a dummy users DataFrame with sample data.
    
    Demonstrates:
    - Basic table structure
    - String, integer, and datetime columns
    """
    users_data = {
        'user_id': [1, 2, 3, 4, 5, 6, 7, 8],
        'username': [
            'alice_smith',
            'bob_jones',
            'carol_white',
            'david_brown',
            'eva_garcia',
            'frank_miller',
            'grace_lee',
            'henry_davis'
        ],
        'email': [
            'alice@example.com',
            'bob@example.com',
            'carol@example.com',
            'david@example.com',
            'eva@example.com',
            'frank@example.com',
            'grace@example.com',
            'henry@example.com'
        ],
        'country': [
            'USA',
            'USA',
            'Canada',
            'USA',
            'Spain',
            'UK',
            'USA',
            'Australia'
        ],
        'signup_date': [
            datetime(2023, 1, 15),
            datetime(2023, 2, 20),
            datetime(2023, 3, 10),
            datetime(2023, 4, 5),
            datetime(2023, 5, 12),
            datetime(2023, 6, 8),
            datetime(2023, 7, 22),
            datetime(2023, 8, 1),
        ],
        'age': [28, 34, 29, 45, 31, 38, 26, 52],
    }
    return pd.DataFrame(users_data)


def create_dummy_orders() -> pd.DataFrame:
    """
    Create a dummy orders DataFrame with sample data.
    
    Demonstrates:
    - Foreign key relationships
    - Numeric and decimal columns
    - Date filtering
    """
    orders_data = {
        'order_id': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112],
        'user_id': [1, 1, 2, 3, 4, 2, 5, 6, 7, 8, 3, 5],
        'product_name': [
            'Laptop',
            'Monitor',
            'Keyboard',
            'Mouse',
            'Headphones',
            'USB Cable',
            'Laptop Stand',
            'Monitor',
            'Keyboard',
            'Headphones',
            'Laptop',
            'Mouse Pad'
        ],
        'amount': [1200.50, 350.00, 89.99, 29.99, 149.99, 15.99, 45.00, 320.00, 95.00, 160.00, 1100.00, 19.99],
        'quantity': [1, 1, 2, 3, 1, 5, 2, 1, 1, 2, 1, 4],
        'order_date': [
            datetime(2024, 1, 5),
            datetime(2024, 1, 15),
            datetime(2024, 1, 8),
            datetime(2024, 1, 12),
            datetime(2024, 1, 20),
            datetime(2024, 2, 3),
            datetime(2024, 2, 10),
            datetime(2024, 2, 15),
            datetime(2024, 2, 18),
            datetime(2024, 2, 25),
            datetime(2024, 3, 5),
            datetime(2024, 3, 10),
        ],
        'status': [
            'completed',
            'completed',
            'completed',
            'pending',
            'completed',
            'completed',
            'shipped',
            'completed',
            'completed',
            'shipped',
            'pending',
            'completed'
        ]
    }
    return pd.DataFrame(orders_data)
