import json

# import mysql.connector
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS


# game detail data

topicsList = ["game_detail", "user_owned_games", "user_friend_list", "user_recently_played_games", "user_idx",
              "user_summary"]


def get_hdfs_dir(topic):
    return "hdfs://localhost:9000/topics/" + topic + "/partition=0/"



if __name__ == '__main__':

    # Load
    # User
    # Owned
    # Games
    spark = SparkSession.builder.appName("games").getOrCreate()

    df_user_owned_games = spark.read.json("hdfs://localhost:9000/topics/user_owned_games/partition=0/*.json").dropDuplicates()
    df_user_owned_games.registerTempTable("user_owned_games")

    df_game_detail = spark.read.json("hdfs://localhost:9000/topics/game_detail/partition=0/*.json").dropDuplicates()
    df_game_detail.registerTempTable("game_detail")
    print("df_game_detail count:")
    print(df_game_detail.count())


    print("top 10 games which have longest total played hours")
    df_global_popular_games = \
        spark.sql("SELECT b.game_id, SUM(b.playtime_forever) AS play_time FROM \
                (SELECT played_games['appid'] AS game_id, played_games['playtime_forever'] AS playtime_forever \
                FROM (SELECT EXPLODE(games) AS played_games FROM user_owned_games) a) b \
                GROUP BY game_id ORDER BY play_time DESC LIMIT 10")
    df_global_popular_games.registerTempTable('popular_games')
    df_global_popular_games.show()
    # find same app id in popular_games and game_detail
    # total played_hours is defined as ranks
    df_global_popular_games = spark.sql("SELECT b.name AS name, a.play_time AS ranks, b.steam_appid, b.header_image FROM \
                                        popular_games a, game_detail b WHERE a.game_id = b.steam_appid ORDER BY ranks DESC")
    print("find same app id in popular_games and game_detail")
    df_global_popular_games.show()
    print("df_global_popular_games count:")
    print(df_global_popular_games.count())

    #df_global_popular_games写入MySQL
    url = 'jdbc:mysql://20.2.129.187/big_data?serverTimezone=Asia/Shanghai'
    mode = 'overwrite'
    df_global_popular_games_properties = {
        "user": "root",
        "password": "111111",
        "driver": 'com.mysql.cj.jdbc.Driver',
        "createTableColumnTypes": "INDEX popular_games_ranks_index (ranks)",
        "truncate": 'true'
    }
    global_popular_games_table = 'popular_games'
    df_global_popular_games.write.jdbc(url=url, mode=mode, properties=df_global_popular_games_properties,
                                       table=global_popular_games_table)



    # #Local
    # Popularity
    # #find his/her friends
    df_user_friend_list = spark.read.json("hdfs://localhost:9000/topics/user_friend_list/partition=0/*.json").dropDuplicates()
    df_user_friend_list.registerTempTable("friend_list")
    sample_user = '76561197960315617'
    df_friend_list = spark.sql("SELECT friends['steamid'] AS steamid FROM \
                (SELECT EXPLODE(friends) AS friends FROM friend_list WHERE steamid = %s) a" % sample_user)
    print("find his/her friends")
    df_friend_list.show(10)
    print("df_friend_list count: ")
    print(df_friend_list.count())
    df_friend_list.registerTempTable('user_friend_list')
    # find out the total playtime of all friends for each game
    temp_local_popular_games = spark.sql("SELECT game_id, SUM(playtime_forever) AS play_time FROM \
                (SELECT games['appid'] AS game_id, games['playtime_forever'] AS playtime_forever FROM \
                (SELECT a.steamid, EXPLODE(b.games) AS games \
                FROM user_friend_list a, user_owned_games b WHERE a.steamid = b.steamid) c) d \
                GROUP BY game_id ORDER BY play_time DESC LIMIT 10")
    temp_local_popular_games.show()
    temp_local_popular_games.registerTempTable('temp_local_popular_games')
    print("temp_local_popular_games count: ")
    print(temp_local_popular_games.count())

    df_global_popular_games = spark.sql("SELECT DISTINCT b.name AS game_name, a.play_time FROM \
                                            temp_local_popular_games a, game_detail b WHERE a.game_id = b.steam_appid")
    print("find out the total playtime of all friends for each game")
    df_global_popular_games.show()
    print("df_global_popular_games count: ")
    print(df_global_popular_games.count())




    # Collaborative
    # Filtering
    # Recommendation
    # System
    df_user_recent_games = spark.read.json("hdfs://localhost:9000/topics/user_recently_played_games/partition=0/*.json").dropDuplicates()
    df_user_recent_games.registerTempTable("user_recent_games")
    df_valid_user_recent_games = spark.sql("SELECT * FROM user_recent_games where total_count != 0")
    df_valid_user_recent_games.show(10)
    print("df_valid_user_recent_games count: ")
    print(df_valid_user_recent_games.count())

    df_user_idx_origin = spark.read.json("hdfs://localhost:9000/topics/user_idx/partition=0/*.json").dropDuplicates().dropDuplicates(['user_id'])
    df_user_idx_origin.registerTempTable('user_idx_origin')
    df_user_idx = spark.sql("SELECT ROW_NUMBER() OVER (ORDER BY user_id) - 1 AS user_idx, user_id FROM user_idx_origin;")
    df_user_idx.registerTempTable('user_idx')
    df_user_idx.show(10)
    print("df_user_idx count: ")
    print(df_user_idx.count())

    df_valid_user_recent_games = spark.sql("SELECT b.user_idx, a.games FROM user_recent_games a \
                                                JOIN user_idx b ON b.user_id = a.steamid WHERE a.total_count != 0")
    print("df_valid_user_recent_games")
    df_valid_user_recent_games.show(10)
    print("f_valid_user_recent_games count: ")
    print(df_valid_user_recent_games.count())



    # ALS
    # map and filter out the games whose playtime is 0
    # training data
    training_rdd = df_valid_user_recent_games.rdd.flatMapValues(lambda x: x) \
        .map(lambda x_y: (x_y[0], x_y[1].appid, x_y[1].playtime_forever)) \
        .filter(lambda x_y_z: x_y_z[2] > 0)
    training_rdd.collect()

    als_model = ALS.trainImplicit(training_rdd, 10)


    # print out 10 recommendeds product for user of index 0
    result_rating = als_model.recommendProducts(0, 10)
    # print result_rating
    try_df_result = spark.createDataFrame(result_rating)
    # print
    print("10 recommendeds product for user of index 0")
    try_df_result.sort("rating", ascending=False).show()


    # 定义一个函数，接受 user_idx 列的值作为参数，并调用 als_model.recommendProducts()
    # 创建 SparkSession
    spark = SparkSession.builder.getOrCreate()


    # 定义一个函数，接受 user_idx 列的值作为参数，并调用 als_model.recommendProducts()
    # def recommend_for_user(user_idx):
    #     recommendations = als_model.recommendProducts(user_idx, 10)
    #     return [{'user_idx': user_idx, 'game_id': row.product, 'rank': idx + 1} for idx, row in
    #             enumerate(recommendations)]
    #
    #
    # # 使用 map() 方法调用 recommend_for_user 函数，并将结果收集为一个列表
    # recommendations_list = df_user_idx.rdd.flatMap(lambda row: recommend_for_user(row.user_idx)).collect()
    #


    # # 将字典列表转换为 DataFrame
    # df_recommend_result = spark.createDataFrame(recommendations_list)
    # def recommend_for_user(row):
    #     user_idx = row.user_idx
    #     recommendations = als_model.recommendProducts(user_idx, 10)
    #     return [{'user_idx': user_idx, 'game_id': row.product, 'rank': idx + 1} for idx, row in
    #             enumerate(recommendations)]
    #
    #
    # recommendations_rdd = df_user_idx.rdd.map(recommend_for_user)
    # recommendations_list = recommendations_rdd.collect()


    # 有个中间数据型要先写入json
    sample_recommended = 'sample_result/sample_recommended.json'
    # write into json
    with open(sample_recommended, 'w') as output_file:
        for user_idx in range(0, df_user_idx.count()):
            try:
                lst_recommended = [i.product for i in als_model.recommendProducts(user_idx, 10)]
                ranks = 1
                for app_id in lst_recommended:
                    dict_recommended = {'user_idx': user_idx, 'game_id': app_id, 'ranks': ranks}
                    json.dump(dict_recommended, output_file)
                    output_file.write('\n')
                    ranks += 1
            # some user index may not in the recommendation result since it's been filtered out
            except:
                pass

    print("存储完成")
    # df_recommend_result = spark.read.json(sample_recommended)
    lst_recommended = []

    for user_idx in range(0, df_user_idx.count()):
        try:
            ranks = 1
            for app_id in [i.product for i in als_model.recommendProducts(user_idx, 10)]:
                dict_recommended = {'user_idx': user_idx, 'game_id': app_id, 'ranks': ranks}
                lst_recommended.append(dict_recommended)
                print(dict_recommended)
                ranks += 1
        except:
            print('异常')
            print(user_idx)
            pass
    print("存储完成，开始读取为dataframe")
    df_recommend_result = spark.createDataFrame(lst_recommended)
    df_recommend_result.show()
    print("sample_result/sample_recommended.json 临时文件表")
    df_recommend_result.show(20)
    print("df_recommend_result count: ")
    print(df_recommend_result.count())

    df_recommend_result.registerTempTable('recommend_result')
    # 这个df_final_recommend_result要存入MySQL
    df_final_recommend_result = spark.sql("SELECT DISTINCT b.user_id, a.ranks, c.name, c.header_image, c.steam_appid \
                                            FROM recommend_result a, user_idx b, game_detail c \
                                            WHERE a.user_idx = b.user_idx AND a.game_id = c.steam_appid \
                                            ORDER BY b.user_id, a.ranks").dropDuplicates().dropDuplicates(['user_id', 'name'])
    print("final_recommend_result")
    df_final_recommend_result.show(20)
    print("df_final_recommend_result count: ")
    print(df_final_recommend_result.count())
    url = 'jdbc:mysql://20.2.129.187/big_data?serverTimezone=Asia/Shanghai'
    mode = 'overwrite'
    df_global_popular_games_properties = {
        "user": "root",
        "password": "111111",
        "driver": 'com.mysql.cj.jdbc.Driver',
        "truncate": 'true'
    }
    final_recommend_result_table = 'personal_recommendation'
    df_final_recommend_result.write.jdbc(url=url, mode=mode, properties=df_global_popular_games_properties, table=final_recommend_result_table)








