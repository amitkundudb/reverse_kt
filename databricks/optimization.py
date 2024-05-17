# Databricks notebook source
# MAGIC %md
# MAGIC Delta Table Optimization Using Compaction

# COMMAND ----------

from pyspark.sql.functions import col
import time

# Create a Delta table path
delta_table_path = "/tmp/delta-table-optimization"

# Generate sample data
data = [(i, j) for i in range(100) for j in range(100)]
columns = ["x", "y"]
df = spark.createDataFrame(data, columns)

# Save the data as a Delta table
# df.write.format("delta").mode("overwrite").save(delta_table_path)

# Function to measure query time
def measure_query_time(query):
    start_time = time.time()
    query.count()  # Just to trigger the query execution
    end_time = time.time()
    return end_time - start_time

# Perform a query before optimization
print("Querying before optimization...")
query_before_optimization = spark.read.format("delta").load(delta_table_path).filter((col("x") == 2) | (col("y") == 2))
time_before_optimization = measure_query_time(query_before_optimization)

# Optimize the Delta table using compaction
print("Optimizing Delta table using compaction...")
spark.sql(f"OPTIMIZE delta.`{delta_table_path}`")

# Perform the same query after compaction
print("Querying after compaction...")
query_after_compaction = spark.read.format("delta").load(delta_table_path).filter((col("x") == 2) | (col("y") == 2))
time_after_compaction = measure_query_time(query_after_compaction)

# Show the performance improvement
print(f"Query time before optimization: {time_before_optimization:.2f} seconds")
print(f"Query time after compaction: {time_after_compaction:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC                                                         Z-Ordering

# COMMAND ----------

from pyspark.sql.functions import col
import time

# Create a Delta table path
delta_table_path = "/tmp/delta-table"

# Generate sample data
data = [(i, j) for i in range(100) for j in range(100)]
columns = ["x", "y"]
df = spark.createDataFrame(data, columns)

# Save the data as a Delta table
# df.write.format("delta").mode("overwrite").save(delta_table_path)

# Function to measure query time
def measure_query_time(query):
    start_time = time.time()
    query.show()
    end_time = time.time()
    return end_time - start_time

# Perform a query without Z-Ordering
print("Querying without Z-Ordering...")
query_no_zorder = spark.read.format("delta").load(delta_table_path).filter((col("x") == 2) | (col("y") == 2))
time_no_zorder = measure_query_time(query_no_zorder)

# Apply Z-Ordering
print("Applying Z-Ordering...")
spark.sql(f"OPTIMIZE delta.`{delta_table_path}` ZORDER BY (x, y)")

# Perform the same query after Z-Ordering
print("Querying with Z-Ordering...")
query_zorder = spark.read.format("delta").load(delta_table_path).filter((col("x") == 2) | (col("y") == 2))
time_zorder = measure_query_time(query_zorder)

# Show the performance improvement
print(f"Query time without Z-Ordering: {time_no_zorder:.2f} seconds")
print(f"Query time with Z-Ordering: {time_zorder:.2f} seconds")


# COMMAND ----------

# MAGIC %md
# MAGIC ACID Transactions

# COMMAND ----------

from pyspark.sql import Row
from delta.tables import DeltaTable

# Create a Delta table path
delta_table_path = "/tmp/delta-table-acid"

# Create sample data
data = [
    Row(id=1, name="Amit", age=25),
    Row(id=2, name="Raj", age=30),
    Row(id=3, name="Priya", age=22)
]
df = spark.createDataFrame(data)

# Save the data as a Delta table
# df.write.format("delta").mode("overwrite").save(delta_table_path)

# Function to print Delta table data
def print_delta_table(table_path):
    df = spark.read.format("delta").load(table_path)
    df.show()

print("Initial Delta table data:")
print_delta_table(delta_table_path)

# Start a transaction
print("Starting a transaction...")

# Load the Delta table
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Update an existing record within a transaction
delta_table.update(condition="id = 1", set={"age": "26"})
print("Updated age for id = 1")

# Insert a new record within a transaction
new_data = [Row(id=4, name="Kiran", age=28)]
new_df = spark.createDataFrame(new_data)
delta_table.alias("oldData").merge(
    new_df.alias("newData"),
    "oldData.id = newData.id"
).whenNotMatchedInsertAll().execute()
print("Inserted new record id = 4")

# Delete a record within a transaction
delta_table.delete(condition="id = 2")
print("Deleted record with id = 2")

# Show the data after transactions
print("Delta table data after transactions:")
print_delta_table(delta_table_path)

# Attempting a failed transaction to demonstrate rollback (consistency)
try:
    print("Attempting a transaction that will fail...")
    delta_table.update(condition="id = 3", set={"age": "invalid_age"})  # Invalid operation
    # delta_table.commit()
except Exception as e:
    print(f"Transaction failed: {e}")
    # delta_table.vacuum(0)
    delta_table.restoreToVersion(1)
    print("Rollback successful")

# Show the data after the failed transaction to demonstrate rollback
print("Delta table data after failed transaction (should be unchanged):")
print_delta_table(delta_table_path)

# COMMAND ----------

l1 = {amit, amit, amit, siya, siya}
new = {0,0,0,1,1}
dic = {amit,siya}


