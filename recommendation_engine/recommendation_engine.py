from pyspark.shell import spark, sc
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType

# game detail data

topicsList = ["game_detail", "user_owned_games", "user_friend_list", "user_recently_played_games", "user_idx",
              "user_summary"]


def get_hdfs_dir(topic):
    return "hdfs://localhost:9000/topics/" + topic + "/partition=0/"


result = [
    '{"type": "demo", "name": "The RPG Engine Demo", "steam_appid": 1818190, "required_age": 0, "is_free": true, "detailed_description": "", "about_the_game": "", "short_description": "", "fullgame": {"appid": "1818180", "name": "The RPG Engine"}, "supported_languages": "English<strong>*</strong><br><strong>*</strong>languages with full audio support", "header_image": "https://cdn.akamai.steamstatic.com/steam/apps/1818190/header.jpg?t=1656270867", "website": null, "pc_requirements": [], "mac_requirements": [], "linux_requirements": [], "developers": ["PolyDemons"], "publishers": ["PolyDemons"], "package_groups": [], "platforms": {"windows": true, "mac": true, "linux": false}, "categories": [{"id": 2, "description": "Single-player"}, {"id": 1, "description": "Multi-player"}, {"id": 9, "description": "Co-op"}, {"id": 38, "description": "Online Co-op"}, {"id": 10, "description": "Game demo"}, {"id": 17, "description": "Includes level editor"}], "release_date": {"coming_soon": false, "date": "5 Jan, 2022"}, "support_info": {"url": "", "email": "polydemons.demos@gmail.com"}, "background": "", "background_raw": "", "content_descriptors": {"ids": [], "notes": null}}']
if __name__ == '__main__':
    # for i in topicsList:
    #     spark = SparkSession.builder \
    #         .appName("Read HDFS Files") \
    #         .getOrCreate()
    #     # 定义 HDFS 目录
    #     hdfs_dir = get_hdfs_dir(i)
    #
    #     # 获取目录下所有文件的文件名
    #     file_content = spark.sparkContext.textFile(hdfs_dir).collect()

    # userid表格
    # rdd = spark.sparkContext.parallelize(result)
    # rows = rdd.map(lambda x: eval(x)).map(lambda x: (x['user_id'],)).distinct().map(lambda x: (x[0],)).map(
    #     lambda x: tuple(x))
    # schema = StructType([StructField("user_id", StringType(), True)])
    # userIdxDf = spark.createDataFrame(rows, schema)

    # user ownd game表格
    # spark = SparkSession.builder.appName("games").getOrCreate()
    #
    # rdd = spark.sparkContext.parallelize(result)
    #
    # userOwnedGameDf = spark.read.json(rdd)
    # userOwnedGameDf.show()
    #
    # userOwnedGameDf = userOwnedGameDf.selectExpr("explode(games) as game") \
    #     .selectExpr("game.appid as appid",
    #                 "game.playtime_forever as playtime_forever",
    #                 "game.playtime_windows_forever as playtime_windows_forever",
    #                 "game.playtime_linux_forever as playtime_linux_forever",
    #                 "game.playtime_mac_forever as playtime_mac_forever",
    #                 "game.rtime_last_played as rtime_last_played")
    #
    # userOwnedGameDf.show()

    # top 10 games which have longest total played hours
    # df_global_popular_games = userOwnedGameDf.selectExpr("appid as game_id",
    #                                                      "playtime_forever as playtime_forever").groupby("game_id").agg(
    #     {"playtime_forever": "sum"}).withColumnRenamed("sum(playtime_forever)", "play_time").orderBy("play_time",
    #                                                                                                  ascending=False).limit(
    #     10)
    # df_global_popular_games.registerTempTable('popular_games')
    # df_global_popular_games.show()

    # find same app id in popular_games and game_detail
    # toatl played_hours is defined as rank



    df_global_popular_games = \
        spark.sql("SELECT b.game_id, SUM(b.playtime_forever) AS play_time FROM \
    (SELECT played_games['appid'] AS game_id, played_games['playtime_forever'] AS playtime_forever \
    FROM (SELECT EXPLODE(games) AS played_games FROM user_owned_games) a) b \
    GROUP BY game_id ORDER BY play_time DESC LIMIT 10")
    df_global_popular_games.registerTempTable('popular_games')

    df_global_popular_games = spark.sql("SELECT b.name AS name, a.play_time AS rank, b.steam_appid, b.header_image FROM \
    popular_games a, game_detail b WHERE a.game_id = b.steam_appid ORDER BY rank DESC")
    df_global_popular_games.show()