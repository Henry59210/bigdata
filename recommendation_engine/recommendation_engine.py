from pyspark.shell import spark
from pyspark.sql import SparkSession

# game detail data

topicsList = ["game_detail", "user_owned_games", "user_friend_list", "user_recently_played_games", "user_idx",
              "user_summary"]

if __name__ == '__main__':
    sc = SparkSession \
        .builder \
        .appName("spark-recommender") \
        .getOrCreate()

    for i in topicsList:
        df = spark.read.json("hdfs://localhost:9000/topics/" + i + "partition=0")
        print(df)
