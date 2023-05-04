from pyspark.shell import spark
from pyspark.sql import SparkSession

from hdfs_utils.load import load_topic

# game detail data

topicsList = ["game_detail", "user_owned_games", "user_friend_list", "user_recently_played_games", "user_idx",
              "user_summary"]

if __name__ == '__main__':
    for i in topicsList:
        result = load_topic(i)
        print(result)
