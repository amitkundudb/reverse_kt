use demo;

-- Create the employees table with
--columns for id, name, department, and salary
CREATE TABLE employees (
   id INT PRIMARY KEY,
   name VARCHAR(255),
   dept VARCHAR(255),
   salary INT
);
-- Insert sample data into the employees table
INSERT INTO employees (id, name, dept, salary) VALUES
    (1, 'Amit kundu', 'Sales', 50000),
    (2, 'Sidhart Shukla', 'Marketing', 60000),
    (3, 'Alex Fridman', 'Sales', 55000),
    (4, 'Virat Kohli', 'HR', 65000),
    (5, 'Andrew Huberman', 'Marketing', 70000);

-- Select all columns and rows from the employees table
SELECT * FROM employees;

-- Select all columns and rows from
--the employees table where the department is 'Sales'
SELECT *
FROM employees
WHERE dept = 'Sales';

-- Select distinct (unique) department values
--from the employees table
SELECT DISTINCT dept
FROM employees;

-- Select the department and the sum of salaries
--for each department, grouped by department
SELECT dept, SUM(salary) AS total_salary
FROM employees
GROUP BY dept;

-- Select the id and name columns for employees in the Sales department
-- This returns a distinct list of ids and names from both Sales and Marketing departments

SELECT id, name
FROM employees
WHERE dept IN ('Sales', 'Marketing');

-- Select the id and name columns for employees in the Sales department
SELECT id, name
FROM employees
WHERE dept = 'Sales'
AND salary > 60000;

--ORDER BY salary
SELECT * FROM employees ORDER BY salary desc;

-- count
SELECT COUNT(*) FROM employees;

-- min, max
SELECT MIN(salary) as lowest_salary,
MAX(salary) as highest_salary FROM employees;

-- average
SELECT AVG(salary) as avg_salary FROM employees;

-- upper
SELECT id,name,salary,UPPER(dept) as dept FROM employees;

-- trim
UPDATE employees
SET name = TRIM(name);

-- To split the name column into first_name and last_name columns

SELECT
    id,
    SUBSTRING(name, 1, CHARINDEX(' ', name) - 1) AS first_name,
    SUBSTRING(name, CHARINDEX(' ', name) + 1, LEN(name)) AS last_name,
    dept,
    salary
FROM employees