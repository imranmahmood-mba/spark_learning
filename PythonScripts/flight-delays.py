from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import functions as F
import os

# Postgres URL
url = "jdbc:postgresql://localhost:5432/pyspark"
# Postgres Credentials
properties = {"user": os.getenv("PG_USER"),
              "password": os.getenv("PG_PASS"),
              "driver":"org.postgresql.Driver"}
# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Flight Delays") \
    .master("local") \
    .getOrCreate()
# Create schema 
schema = StructType([
    StructField("date", DateType(), True),
        StructField("delay", IntegerType()), 
        StructField("distance", IntegerType()),
        StructField("origin", StringType()), 
        StructField("destination", StringType())
])
 # Create DataFrame
df = spark.read.csv("../LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv", header=True, schema=schema)

# test grouping
sum_destination_grouped_df = df.groupBy("destination").sum("delay")
avg_destination_grouped_df = df.groupBy("destination").agg(F.avg("delay").alias("avg_delay")).withColumn("avg_delay", F.round(F.col("avg_delay"), 2))
sum_longest_trip_df = df.groupBy(["origin", "destination"]).agg(F.avg("delay").alias("avg_delay")).withColumn("avg_delay", F.round(F.col("avg_delay"), 2))
df_filtered = df.filter(df.delay > 100).show()
# Displays the content of the DataFrame to stdout
sum_destination_grouped_df.show()
avg_destination_grouped_df.show()
sum_longest_trip_df.orderBy(["origin", "destination"]).write.jdbc(url=url, table="flight_delay_aggregate", mode="overwrite", properties=properties)
