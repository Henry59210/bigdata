from pyspark.sql import SparkSession

def load_topic(topic):
    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("Read HDFS Files") \
        .getOrCreate()

    # 定义 HDFS 目录
    hdfs_dir = "hdfs://localhost:9000/topics/" + topic + "/partition=0/"

    # 获取目录下所有文件的文件名
    file_names = spark.sparkContext.textFile(hdfs_dir).collect()
    return file_names


if __name__ == '__main__':
    print(load_topic('user_idx'))