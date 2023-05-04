from pyspark.shell import spark
from pyspark.sql import SparkSession

# game detail data
game_detail = 'data/game_detail.json'

# sample data for popularity recommendation
sample_user_owned_games = 'data/user_owned_games_sample.json'
sample_user_friend_list = 'data/user_friend_list_sample.json'

# sample data for collaborative filtering recommendation
sample_user_recent_games = 'data/user_recently_played_games_sample.json'
sample_user_idx = 'data/user_idx_sample.json'

# output files using sample data
sample_recommended = 'result/sample_recommended.json'
sample_final_recommended = 'result/sample_final_recommended'


if __name__ == '__main__':
    sc = SparkSession \
        .builder \
        .appName("spark-recommender") \
        .getOrCreate()

    df = spark.read.json("hdfs://20.2.240.50:9000/topics/user_idx/partition=0")
    print(df)

