example table orders

# Create Database

CREATE DATABASE IF NOT EXISTS order_db;
USE order_db;

# Create Table orders

CREATE TABLE IF NOT EXISTS orders (
id INT AUTO_INCREMENT PRIMARY KEY,
order_id VARCHAR(255) NOT NULL,
product_name VARCHAR(255) NOT NULL,
quantity INT NOT NULL,
price DECIMAL(10,2) NOT NULL,
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
