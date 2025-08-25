import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to MySQL
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="pgbto2W5",
    database="Ecommerce"
)
cur = db.cursor()

# 1. List all unique cities where customers are located.
query = """
SELECT DISTINCT customer_city
FROM customers;
"""
cur.execute(query)
data = cur.fetchall()
# print(data)

# 2. Count the number of orders placed in 2017.
query = """
SELECT COUNT(order_id)
FROM orders
WHERE YEAR(order_purchase_timestamp) = 2017;
"""
cur.execute(query)
data = cur.fetchall()
# print(data[0][0])

# 3. Find the total sales per category.
query = """
SELECT 
    products.product_category AS category,
    ROUND(SUM(payments.payment_value), 2) AS sales
FROM products
JOIN order_item
    ON products.product_id = order_item.product_id
JOIN payments
    ON payments.order_id = order_item.order_id
GROUP BY products.product_category;
"""
cur.execute(query)
data = cur.fetchall()
# print(data)

# 4. Calculate the percentage of orders that were paid in installments.
query = """
SELECT 
    (SUM(CASE WHEN payment_installments >= 1 THEN 1 ELSE 0 END) / COUNT(*) * 100.0) AS percentage
FROM payments;
"""
cur.execute(query)
data = cur.fetchall()
# print(data[0][0])

# 5. Count the number of customers from each state.
query = """
SELECT customer_state, COUNT(customer_id) AS customer_count
FROM customers
GROUP BY customer_state;
"""
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data, columns=['state', 'count'])
# print(df)

# 6. Calculate the number of orders per month in 2018.
query = """
SELECT 
    MONTH(order_purchase_timestamp) AS month_num,
    MONTHNAME(order_purchase_timestamp) AS month_name,
    COUNT(order_id) AS order_count
FROM orders
WHERE YEAR(order_purchase_timestamp) = 2018
GROUP BY month_num, month_name
ORDER BY month_num;
"""
cur.execute(query)
data = cur.fetchall()
# df = pd.DataFrame(data, columns=["month_num", "month_name", "order_count"])

# 7. Find the average number of products per order, grouped by customer city.
query = """
WITH count_per_order AS (
    SELECT 
        orders.order_id, 
        orders.customer_id, 
        COUNT(order_item.order_id) AS oc
    FROM orders 
    JOIN order_item
        ON orders.order_id = order_item.order_id
    GROUP BY orders.order_id, orders.customer_id
)
SELECT 
    customers.customer_city,
    ROUND(AVG(count_per_order.oc), 2) AS average_orders
FROM customers 
JOIN count_per_order
    ON customers.customer_id = count_per_order.customer_id
GROUP BY customers.customer_city
ORDER BY average_orders DESC;
"""
cur.execute(query)
data = cur.fetchall()
# print(data)

# 8. Calculate the percentage of total revenue contributed by each product category.
query = """
SELECT 
    products.product_category AS category,
    ROUND(
        (SUM(payments.payment_value) / 
        (SELECT SUM(payment_value) FROM payments) * 100), 2
    ) AS sales_percentage
FROM products 
JOIN order_item
    ON products.product_id = order_item.product_id
JOIN payments
    ON payments.order_id = order_item.order_id
GROUP BY products.product_category;
"""
cur.execute(query)
data = cur.fetchall()
print(data)


