from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("RedditDataProcessing") \
    .master("spark://192.168.2.39:7077") \
    .config("spark.shuffle.service.enabled", True)\
    .config("spark.executor.instances", 4) \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", 4) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
# files path
reddit_path = "hdfs://192.168.2.156:9000/data/reddit/reddit_50k.json"
celebrities_path = "hdfs://192.168.2.156:9000/output/group31/Formatted_Celebrities.csv"

# read data
reddit = spark.read.json(reddit_path)
names_rdd = spark.read.csv(celebrities_path, header = True, inferSchema = True)
print("names_rdd")
print(names_rdd)
name_df = names_rdd.select(col("name").alias("name"))

print("names_df")
name_df.show(5)

columns_to_keep = ["normalizedBody", "subreddit", "summary"]
reddit_selected = reddit.select(columns_to_keep)
reddit_selected = reddit_selected.dropna()
reddit_selected = reddit_selected.withColumn("normalizedBody", lower(col("normalizedBody")))

reddit_selected.show(5)

# use crossJoin + contains() search for names in the normalizedBody
joined_df = reddit_selected.crossJoin(name_df)
matched_df = joined_df.filter(col("normalizedBody").contains(col("name")))

# count the number of times each celebrity is mentioned
count_df = matched_df.groupBy("name").count().withColumnRenamed("count", "times")
count_df.show(5)

output_path = "hdfs://192.168.2.156:9000/output/group31/celebrity_counts.csv"
count_df.write.csv(output_path, header=True, mode="overwrite")

spark.stop()
