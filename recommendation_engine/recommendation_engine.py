from pyspark.shell import spark
from pyspark.sql import SparkSession

from hdfs_utils.load import load_topic

# game detail data

topicsList = ["game_detail", "user_owned_games", "user_friend_list", "user_recently_played_games", "user_idx",
              "user_summary"]

if __name__ == '__main__':
    for i in topicsList:
        spark = SparkSession.builder \
            .appName("Read HDFS Files") \
            .getOrCreate()

        # 定义 HDFS 目录
        hdfs_dir = "hdfs://localhost:9000/topics/" + i + "/partition=0/"

        # 获取目录下所有文件的文件名
        file_names = spark.sparkContext.textFile(hdfs_dir).collect()
        print(file_names)
