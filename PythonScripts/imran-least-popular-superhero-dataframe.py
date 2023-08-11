from pyspark.sql import functions as func
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, IntegerType, StructField, DataType
import codecs 

def get_hero_id():
    with codecs.open("../Marvel+Graph", "r", encoding='ISO-8859-1', errors='ignore') as f:
        friend_list = []
        for line in f:
            ids = line.split(' ')
            first_id = ids[0]
            other_ids = ids[1:]
            number_of_friends = len(other_ids)
            try:
                friend_list.append({'heroID':int(first_id), 'num_of_friends':int(number_of_friends)})
            except ValueError:
                print(f"Failed to parse line: {line}")
        return friend_list
def create_hero_names():
    with codecs.open("../Marvel+Names", "r", encoding='ISO-8859-1', errors='ignore') as f:
        hero_data = []
        for line in f:
            line = line.split(' ')
            heroID = line[0]
            name = line[1:]
            hero_data.append({"heroID":heroID, "name":name})
        return hero_data

spark = SparkSession.builder.appName('Marvel').master('local').getOrCreate()

# Read in the hero name dataset
hero_name_df = spark.createDataFrame(create_hero_names())

# Get the number of friends per hero
df = spark.createDataFrame(get_hero_id())
df = df.groupBy("heroID").sum("num_of_friends")
df = df.select(func.col("sum(num_of_friends)").alias("friends"), func.col("heroID").alias("heroID"))

# Get the smallest amount of friends
friends_min = df.select(func.min("friends")).first()[0]

# Filter hero_name_df to get all heroes with the min number of friends
df_joined = df.join(hero_name_df, "heroID")
df_joined.filter(df_joined.friends == friends_min).select(df_joined.name).orderBy(df_joined.name).show()