# Databricks notebook source
# Create the employees DataFrame
employees = spark.createDataFrame([
    (1, 'Amit kundu', 'Sales', 50000),
    (2, 'Sidhart Shukla', 'Marketing', 60000),
    (3, 'Alex Fridman', 'Sales', 55000),
    (4, 'Virat Kohli', 'HR', 65000),
    (5, 'Andrew Huberman', 'Marketing', 70000)
], ['id', 'name', 'dept', 'salary'])

# Show all columns and rows from the employees DataFrame
employees.display()

# COMMAND ----------

# Select all columns and rows where the department is 'Sales'
employees.filter(employees.dept == 'Sales').display()

# COMMAND ----------

# Select unique department values
employees.select('dept').distinct().display()

# COMMAND ----------


# Select the department and the sum of salaries for each department
employees.groupBy('dept').sum('salary').display()

# COMMAND ----------

# Select the id and name columns for employees in the Sales and Marketing departments
employees.filter(employees.dept.isin(['Sales', 'Marketing'])) \
          .select('id', 'name') \
          .display()

# COMMAND ----------


# Select the id and name columns for employees in the Sales department with salary > 60000
sales_high_salary = employees.filter((employees.dept == 'Sales') & (employees.salary > 60000)) \
                            .select('id', 'name')
sales_high_salary.show()

# COMMAND ----------

from pyspark.sql.functions import min, max, mean, count

# Order by highest salary
result = employees.orderBy(employees["salary"].desc())
result.display()
# count
result = employees.count()
# min, max
min_salary = employees.select(min("salary")).collect()[0][0]
max_salary = employees.select(max("salary")).collect()[0][0]
# average
avg_salary = employees.select(mean("salary")).collect()[0][0]

print(result)
print("Minimum salary is ",min_salary)
print("Maximum salary is ",max_salary)
print("Average salary is ",avg_salary)

# COMMAND ----------

from pyspark.sql.functions import split, col

# Split 'name' column into 'first_name' and 'last_name' columns
splitted_df = employees.withColumn("first_name", split(col("name"), " ")[0]) \
                           .withColumn("last_name", split(col("name"), " ")[1]).drop(col("name"))

# Show the updated DataFrame
splitted_df.display()


# COMMAND ----------

# Creating sample DataFrames for orders and customers
orders_data = [(1, 101), (2, 102), (3, 103),(4,104)]
orders_columns = ["order_id", "customer_id"]
orders_df = spark.createDataFrame(orders_data, orders_columns)

customers_data = [(101, 'Amit Kundu'), (102, 'Virat Kohli'), (103, 'Alex Fridman')]
customers_columns = ["customer_id", "customer_name"]
customers_df = spark.createDataFrame(customers_data, customers_columns)

orders_df.display()
customers_df.display()


## INNER JOIN
joined_df = orders_df.join(customers_df, orders_df.customer_id == customers_df.customer_id, "inner") \
                     .select(orders_df.order_id, orders_df.customer_id, customers_df.customer_name)
joined_df.display()

## UNION
unioned_df = orders_df.select("customer_id") \
                      .union(customers_df.select("customer_id"))
unioned_df.distinct().display()

