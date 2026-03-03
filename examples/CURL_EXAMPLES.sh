#!/bin/bash

# DuckDB Query API - cURL Examples
# Run individual commands to test different SQL features
# Make sure the API is running at http://localhost:8000

BASE_URL="http://localhost:8000"

echo "========================================="
echo "DuckDB Query API - cURL Examples"
echo "========================================="

# Health Check
echo -e "\n[1] Health Check"
curl -s "$BASE_URL/health" | jq '.'

# Get Schema
echo -e "\n[2] Database Schema"
curl -s "$BASE_URL/schema" | jq '.schema | keys'

# Get Users Table Sample
echo -e "\n[3] Users Table Sample (First 3 rows)"
curl -s "$BASE_URL/tables/users?limit=3" | jq '.data'

# Get Orders Table Sample
echo -e "\n[4] Orders Table Sample (First 3 rows)"
curl -s "$BASE_URL/tables/orders?limit=3" | jq '.data'

# ============================================
# BASIC QUERIES
# ============================================

echo -e "\n\n========== BASIC QUERIES =========="

# SELECT ALL
echo -e "\n[5] SELECT All Users"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT user_id, username, country FROM users","params":[]}' | jq '.data'

# SELECT with LIMIT
echo -e "\n[6] SELECT with LIMIT"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM orders LIMIT 3","params":[]}' | jq '.data'

# ============================================
# WHERE CLAUSE
# ============================================

echo -e "\n\n========== WHERE CLAUSE =========="

# WHERE - Simple condition
echo -e "\n[7] WHERE - Users from USA"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT username, email, country FROM users WHERE country = ?","params":["USA"]}' | jq '.data'

# WHERE - Numeric comparison
echo -e "\n[8] WHERE - Users older than 30"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT username, age FROM users WHERE age > ?","params":[30]}' | jq '.data'

# WHERE - AND condition
echo -e "\n[9] WHERE - AND (USA users older than 30)"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT username, age, country FROM users WHERE country = ? AND age > ?","params":["USA", 30]}' | jq '.data'

# WHERE - OR condition
echo -e "\n[10] WHERE - OR (Canada or Spain users)"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT username, country FROM users WHERE country = ? OR country = ?","params":["Canada", "Spain"]}' | jq '.data'

# WHERE - IN operator
echo -e "\n[11] WHERE - IN (Users from multiple countries)"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT username, country FROM users WHERE country IN (?, ?, ?)","params":["USA", "Canada", "UK"]}' | jq '.data'

# ============================================
# ORDER BY
# ============================================

echo -e "\n\n========== ORDER BY =========="

# ORDER BY DESC
echo -e "\n[12] ORDER BY DESC - Oldest users first"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT username, age FROM users ORDER BY age DESC LIMIT 5","params":[]}' | jq '.data'

# ORDER BY ASC
echo -e "\n[13] ORDER BY ASC - Youngest users first"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT username, age FROM users ORDER BY age ASC LIMIT 5","params":[]}' | jq '.data'

# ORDER BY multiple columns
echo -e "\n[14] ORDER BY Multiple Columns"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT country, username, age FROM users ORDER BY country ASC, age DESC LIMIT 8","params":[]}' | jq '.data'

# ============================================
# GROUP BY & AGGREGATIONS
# ============================================

echo -e "\n\n========== GROUP BY & AGGREGATIONS =========="

# GROUP BY - COUNT
echo -e "\n[15] GROUP BY - Users per Country"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT country, COUNT(*) as user_count FROM users GROUP BY country ORDER BY user_count DESC","params":[]}' | jq '.data'

# GROUP BY - AVG
echo -e "\n[16] GROUP BY - Average Age by Country"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT country, AVG(age) as avg_age FROM users GROUP BY country ORDER BY avg_age DESC","params":[]}' | jq '.data'

# Multiple aggregations
echo -e "\n[17] GROUP BY - Multiple Aggregations by Country"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT country, COUNT(*) as user_count, AVG(age) as avg_age, MIN(age) as min_age, MAX(age) as max_age FROM users GROUP BY country","params":[]}' | jq '.data'

# HAVING clause
echo -e "\n[18] GROUP BY with HAVING - Countries with 2+ users"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT country, COUNT(*) as user_count FROM users GROUP BY country HAVING COUNT(*) > ? ORDER BY user_count DESC","params":[1]}' | jq '.data'

# ORDER AGGREGATIONS
echo -e "\n[19] Orders - Total by Status"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT status, COUNT(*) as count, SUM(amount) as total_amount FROM orders GROUP BY status","params":[]}' | jq '.data'

# Product Statistics
echo -e "\n[20] Product Statistics - Price Range"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT product_name, COUNT(*) as sold, AVG(amount) as avg_price, MIN(amount) as min_price, MAX(amount) as max_price FROM orders GROUP BY product_name HAVING COUNT(*) > ?","params":[1]}' | jq '.data'

# ============================================
# DISTINCT
# ============================================

echo -e "\n\n========== DISTINCT =========="

# DISTINCT values
echo -e "\n[21] DISTINCT - All countries"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT DISTINCT country FROM users ORDER BY country","params":[]}' | jq '.data'

# DISTINCT order statuses
echo -e "\n[22] DISTINCT - Order statuses"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT DISTINCT status FROM orders ORDER BY status","params":[]}' | jq '.data'

# ============================================
# JOINS
# ============================================

echo -e "\n\n========== JOINS =========="

# INNER JOIN
echo -e "\n[23] INNER JOIN - User Orders"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT u.username, o.product_name, o.amount, o.order_date FROM users u INNER JOIN orders o ON u.user_id = o.user_id LIMIT 5","params":[]}' | jq '.data'

# INNER JOIN with WHERE
echo -e "\n[24] INNER JOIN with WHERE - USA User Orders"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT u.username, u.country, o.product_name, o.amount FROM users u INNER JOIN orders o ON u.user_id = o.user_id WHERE u.country = ?","params":["USA"]}' | jq '.data'

# LEFT JOIN
echo -e "\n[25] LEFT JOIN - All Users with Order Count"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT u.username, COUNT(o.order_id) as order_count FROM users u LEFT JOIN orders o ON u.user_id = o.user_id GROUP BY u.username ORDER BY order_count DESC","params":[]}' | jq '.data'

# LEFT JOIN with SUM
echo -e "\n[26] LEFT JOIN - User Total Spending"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT u.username, COALESCE(SUM(o.amount), 0) as total_spent FROM users u LEFT JOIN orders o ON u.user_id = o.user_id GROUP BY u.username ORDER BY total_spent DESC","params":[]}' | jq '.data'

# Complex JOIN
echo -e "\n[27] Complex JOIN - Orders by Country and Status"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT u.country, o.status, COUNT(o.order_id) as order_count, SUM(o.amount) as total_amount FROM users u LEFT JOIN orders o ON u.user_id = o.user_id GROUP BY u.country, o.status ORDER BY u.country, total_amount DESC","params":[]}' | jq '.data'

# ============================================
# CASE STATEMENTS
# ============================================

echo -e "\n\n========== CASE STATEMENTS =========="

# CASE - Age groups
echo -e "\n[28] CASE - User Age Groups"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT username, age, CASE WHEN age < 30 THEN '\''Young'\'' WHEN age < 45 THEN '\''Adult'\'' ELSE '\''Senior'\'' END as age_group FROM users ORDER BY age","params":[]}' | jq '.data'

# CASE - Order amount categorization
echo -e "\n[29] CASE - Order Size Categories"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT product_name, amount, CASE WHEN amount < 50 THEN '\''Budget'\'' WHEN amount < 500 THEN '\''Standard'\'' ELSE '\''Premium'\'' END as price_category FROM orders","params":[]}' | jq '.data'

# ============================================
# AGGREGATE FUNCTIONS
# ============================================

echo -e "\n\n========== AGGREGATE FUNCTIONS =========="

# COUNT
echo -e "\n[30] COUNT - Total counts"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT COUNT(*) as total_users, COUNT(DISTINCT country) as countries FROM users","params":[]}' | jq '.data'

# SUM, AVG, MIN, MAX
echo -e "\n[31] Order Value Statistics"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT COUNT(*) as total_orders, SUM(amount) as total_revenue, AVG(amount) as avg_order, MIN(amount) as min_order, MAX(amount) as max_order FROM orders","params":[]}' | jq '.data'

# ============================================
# SUBQUERIES
# ============================================

echo -e "\n\n========== SUBQUERIES =========="

# Subquery with IN
echo -e "\n[32] Subquery - Users with orders > 500"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT username FROM users WHERE user_id IN (SELECT user_id FROM orders WHERE amount > ?)","params":[500]}' | jq '.data'

# Subquery with GROUP BY
echo -e "\n[33] Subquery - Heavy spenders (2+ orders)"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT username FROM users WHERE user_id IN (SELECT user_id FROM orders GROUP BY user_id HAVING COUNT(*) > ?)","params":[1]}' | jq '.data'

# ============================================
# DATE FILTERING
# ============================================

echo -e "\n\n========== DATE FILTERING =========="

# Orders from specific date
echo -e "\n[34] Orders from January 2024"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM orders WHERE order_date >= '\''2024-01-01'\'' AND order_date < '\''2024-02-01'\''","params":[]}' | jq '.data'

# ============================================
# COMPLEX QUERIES
# ============================================

echo -e "\n\n========== COMPLEX QUERIES =========="

# Revenue by user and country
echo -e "\n[35] Revenue Report - By User"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT u.username, u.country, COUNT(o.order_id) as orders, SUM(o.amount) as total_spent, AVG(o.amount) as avg_order FROM users u LEFT JOIN orders o ON u.user_id = o.user_id GROUP BY u.user_id, u.username, u.country ORDER BY total_spent DESC","params":[]}' | jq '.data'

# Popular products
echo -e "\n[36] Popular Products by Revenue"
curl -s -X POST "$BASE_URL/query" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT product_name, COUNT(*) as times_ordered, SUM(amount) as total_revenue, AVG(amount) as avg_price FROM orders GROUP BY product_name ORDER BY total_revenue DESC","params":[]}' | jq '.data'

echo -e "\n========================================="
echo "Examples completed! ✓"
echo "========================================="
