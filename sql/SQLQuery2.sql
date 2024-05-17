use demo;

-- Creating sample tables
CREATE TABLE orders (
    order_id INT,
    customer_id INT
);

CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(50)
);

-- Inserting sample data into orders table
INSERT INTO orders (order_id, customer_id)
VALUES
    (1, 101),
    (2, 102),
    (3, 103),
	(4, 104);

-- Inserting sample data into customers table
INSERT INTO customers (customer_id, customer_name)
VALUES
    (101, 'Amit Kundu'),
	(102, 'Virat Kohli'),
	(103, 'Alex Fridman');

--INNER JOIN
SELECT orders.order_id, orders.customer_id, customers.customer_name
FROM orders
JOIN customers ON orders.customer_id = customers.customer_id;

--UNION
SELECT customer_id FROM orders
UNION
SELECT customer_id FROM customers;


