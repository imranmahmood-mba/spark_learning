from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import functions as F

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("Flight Delays") \
    .master("local") \
    .getOrCreate()

schema = StructType([
    StructField("date", DateType(), True),
        StructField("delay", IntegerType()), 
        StructField("distance", IntegerType()),
        StructField("string", StringType()), 
        StructField("destination", StringType())
])
# Now you can start using the DataFrame API
df = spark.read.csv("../LearningSparkV2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv", header=True, schema=schema)

# test grouping
sum_destination_grouped_df = df.groupBy("destination").sum("delay")
avg_destination_grouped_df = df.groupBy("destination").agg(F.avg("delay").alias("avg_delay")).withColumn("avg_delay", F.round(F.col("avg_delay"), 2))


# Displays the content of the DataFrame to stdout
sum_destination_grouped_df.show()
avg_destination_grouped_df.show()