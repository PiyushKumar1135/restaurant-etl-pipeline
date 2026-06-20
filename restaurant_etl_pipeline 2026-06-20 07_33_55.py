# Databricks notebook source
print("Setup done")

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/etl_data/Dataset .csv", header=True, inferSchema=True)
df.show(5)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, avg, count, round, expr


df_clean = df.dropna(subset=["Restaurant ID", "City", "Cuisines", "Aggregate rating"]) \
    .withColumn("Aggregate rating", expr("try_cast(`Aggregate rating` as double)")) \
    .withColumn("Average Cost for two", expr("try_cast(`Average Cost for two` as double)"))


city_summary = df_clean.groupBy("City").agg(
    round(avg("Aggregate rating"), 2).alias("avg_rating"),
    round(avg("Average Cost for two"), 2).alias("avg_cost"),
    count("Restaurant ID").alias("total_restaurants")
).orderBy(col("total_restaurants").desc())

city_summary.show(10)


cuisine_summary = df_clean.groupBy("Cuisines").agg(
    count("Restaurant ID").alias("restaurant_count"),
    round(avg("Aggregate rating"), 2).alias("avg_rating")
).orderBy(col("restaurant_count").desc())

cuisine_summary.show(10)

# COMMAND ----------


city_summary.write.mode("overwrite").saveAsTable("workspace.default.city_summary")
cuisine_summary.write.mode("overwrite").saveAsTable("workspace.default.cuisine_summary")

print("Tables created successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.city_summary ORDER BY total_restaurants DESC LIMIT 10