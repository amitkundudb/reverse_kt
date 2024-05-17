# Databricks notebook source
# MAGIC %md
# MAGIC Tranformations on RDD

# COMMAND ----------

rdd = sc.parallelize([1, 2, 3, 4, 5])
# Map (on RDD)
mapped_rdd = rdd.map(lambda x: x * 2)
print("Map (on RDD):")
print(mapped_rdd.collect())  # Output: [2, 4, 6, 8, 10]

# Filter (on RDD)
filtered_rdd = rdd.filter(lambda x: x % 2 == 0)
print("\nFilter (on RDD):")
print(filtered_rdd.collect())  # Output: [2, 4]

# FlatMap (on RDD)
flat_mapped_rdd = rdd.flatMap(lambda x: [x, x + 1])
print("\nFlatMap (on RDD):")
print(flat_mapped_rdd.collect())  # Output: [1, 2, 2, 3, 3, 4, 4, 5, 5, 6]

# COMMAND ----------

# Union (on RDD)
union_rdd = rdd.union(sc.parallelize([6, 7, 8]))
print("\nUnion (on RDD):")
print(union_rdd.collect())  # Output: [1, 2, 3, 4, 5, 6, 7, 8]

# Intersection (on RDD)
intersection_rdd = rdd.intersection(sc.parallelize([3, 4, 5, 6, 7]))
print("\nIntersection (on RDD):")
print(intersection_rdd.collect())  # Output: [3, 4, 5]

# COMMAND ----------

# Distinct (on RDD)
distinct_rdd = rdd.distinct()
print("\nDistinct (on RDD):")
print(distinct_rdd.collect())  # Output: [1, 2, 3, 4, 5]

# Sample (on RDD)
sampled_rdd = rdd.sample(withReplacement=False, fraction=0.5)
print("\nSample (on RDD):")
print(sampled_rdd.collect())  # Output: Random sample of 50% of the RDD

# Repartition (on RDD)
repartitioned_rdd = rdd.repartition(2)
print("\nRepartition (on RDD):")
print(f"RDD repartitioned into {repartitioned_rdd.getNumPartitions()} partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC Transformations on DataFrame

# COMMAND ----------

data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35), (4, "David", 40), (5, "Eve", 45)]
df = spark.createDataFrame(data, ["id", "name", "age"])

# Map (on DataFrame)
mapped_df = df.rdd.map(lambda row: (row.id, row.name, row.age * 2)).toDF(["id", "name", "age"])
print("\nMap (on DataFrame):")
mapped_df.show()

# Filter (on DataFrame)
filtered_df = df.filter("age > 30")
print("\nFilter (on DataFrame):")
filtered_df.show()

# COMMAND ----------

# FlatMap (on DataFrame)
flat_mapped_df = df.rdd.flatMap(lambda row: [(row.id, row.name, row.age), (row.id, row.name, row.age + 1)]).toDF(["id", "name", "age"])
print("\nFlatMap (on DataFrame):")
flat_mapped_df.show()

# Union (on DataFrame)
union_df = df.union(spark.createDataFrame([(6, "Frank", 50), (7, "Grace", 55), (8, "Henry", 60)], ["id", "name", "age"]))
print("\nUnion (on DataFrame):")
union_df.show()

# COMMAND ----------

# Intersection (on DataFrame)
intersection_df = df.intersect(spark.createDataFrame([(3, "Charlie", 35), (4, "David", 40), (5, "Eve", 45), (6, "Frank", 50), (7, "Grace", 55)], ["id", "name", "age"]))
print("\nIntersection (on DataFrame):")
intersection_df.show()

# Distinct (on DataFrame)
distinct_df = df.distinct()
print("\nDistinct (on DataFrame):")
distinct_df.show()

# Sample (on DataFrame)
sampled_df = df.sample(withReplacement=False, fraction=0.5)
print("\nSample (on DataFrame):")
sampled_df.show()

# Repartition (on DataFrame)
repartitioned_df = df.repartition(2)
print("\nRepartition (on DataFrame): {repartitioned_df.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC Only Applies to RDD

# COMMAND ----------

list = [15,14,13,12,11,10,9,8,7,6,5,4,3,2,1]
rdd = sc.parallelize(list,3)
print(f"This is list {rdd.collect()}")

#mapPartitions()
def add_one(iterator):
    return map(lambda x: x + 1, iterator)
result_rdd = rdd.mapPartitions(add_one)
print(f"After adding 1 with each element {result_rdd.collect()}")

#mapPartitionsWithIndex()
def add_partition_index(index, iterator):
    return map(lambda x: (index, x), iterator)
indexed_rdd = rdd.mapPartitionsWithIndex(add_partition_index)
print(f"Elements with index of partition {indexed_rdd.collect()}")

# Coalesce
coalesced_rdd = rdd.coalesce(2)
print("\nCoalesce:")
print(f"RDD coalesced into {coalesced_rdd.getNumPartitions()} partitions")

# COMMAND ----------

# MAGIC %md
# MAGIC Only Applies to DataFrame

# COMMAND ----------

data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35), (4, "David", 40), (5, "Eve", 45)]
df = spark.createDataFrame(data, ["id", "name", "age"])

# Create another DataFrame
data2 = [(1, "wb"), (2, "mb"), (3, "us"), (4, "ny"), (5, "sf")]
df2 = spark.createDataFrame(data2, ["id", "address"])

# Join
joined_df = df.join(df2, on="id", how="inner")
print("\nJoin:")
joined_df.display()

# Sort
sorted_df = df.sort("age", "name")
print("\nSort:")
sorted_df.display()

# Alias
aliased_df = df.select(df.name.alias("fullName"))
print("\nAlias:")
aliased_df.display()

# WithColumn
with_column_df = df.withColumn("age_plus_10", df.age + 10)
print("\nWithColumn:")
with_column_df.display()

# Drop
dropped_df = df.drop("id")
print("\nDrop:")
dropped_df.display()
