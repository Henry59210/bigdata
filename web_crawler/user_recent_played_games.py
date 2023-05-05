import json
import sys
import requests
from pyspark.mllib.recommendation import MatrixFactorizationModel
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS

key = '8F8BBCEDF2B6E75EDC1F65A9DADB9A0E'
url = 'http://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v0001/?key=' + key + '&steamid='


def get_user_recent_played_games(user_id):
    url_temp = url + str(user_id)
    print(url_temp)
    resp = requests.get(url_temp)

    # resp = requests.head(url_temp)
    obj = process_json_obj(resp, user_id)
    # json.dump(obj, f)
    return obj


def process_json_obj(resp, user_id):
    try:
        obj = resp.json()['response']
        obj = {'steamid': user_id, 'total_count': obj['total_count'], 'games': obj['games']}
    except Exception as e:
        # corner case: total_count is zero
        print(e)
        if 'total_count' in obj:
            obj = {'steamid': user_id, 'total_count': obj['total_count'], 'games': []}
        else:
            obj = {'steamid': user_id, 'total_count': -1, 'games': []}
    return json.dumps(obj)


def dump_file(output_path, obj):
    with open(output_path, 'w') as f:
        f.write(obj)
        f.write('\n')


def append(df):
    user_idx_file = 'user_idx_file.json'
    user_recent_games_file = 'user_recent_games_file.json'
    user_id = sys.argv[1]
    user_recent_played_games = get_user_recent_played_games(user_id)
    spark = SparkSession.builder.appName("games").getOrCreate()
    user_idx_str = '{"user_idx": 0, "user_id": ' + user_id + '}'
    dump_file(user_idx_file, user_idx_str)
    dump_file(user_recent_games_file, user_recent_played_games)
    df_user_idx_append = spark.read.json(user_idx_file)
    df_user_idx_append.registerTempTable("user_idx1")
    df_user_idx_append.show()
    df_user_recent_games_append = spark.read.json(user_recent_games_file)
    df_user_recent_games_append.registerTempTable("user_recent_games1")
    df_user_recent_games_append.show()
    df_valid_user_recent_games_append = spark.sql("SELECT b.user_idx, a.games FROM user_recent_games1 a \
                                                    JOIN user_idx1 b ON b.user_id = a.steamid WHERE a.total_count != 0")
    return df.union(df_valid_user_recent_games_append)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python main.py <user_id>")
    else:
        spark = SparkSession.builder.appName("games").getOrCreate()
        # Collaborative
        # Filtering
        # Recommendation
        # System
        df_user_recent_games = spark.read.json(
            "hdfs://localhost:9000/topics/user_recently_played_games/partition=0/*.json").dropDuplicates()
        df_user_recent_games.registerTempTable("user_recent_games")
        df_valid_user_recent_games = spark.sql("SELECT * FROM user_recent_games where total_count != 0")
        df_valid_user_recent_games.show(1)
        print("df_valid_user_recent_games count: ")
        print(df_valid_user_recent_games.count())

        df_user_idx = spark.read.json("hdfs://localhost:9000/topics/user_idx/partition=0/*.json").dropDuplicates()
        df_user_idx.registerTempTable('user_idx')
        df_valid_user_recent_games = spark.sql("SELECT b.user_idx, a.games FROM user_recent_games a \
                                                       JOIN user_idx b ON b.user_id = a.steamid WHERE a.total_count != 0")
        df_valid_user_recent_games = append(df_valid_user_recent_games)
        print("df_valid_user_recent_games")
        df_valid_user_recent_games.show(10)
        print("f_valid_user_recent_games count: ")
        print(df_valid_user_recent_games.count())

        # map and filter out the games whose playtime is 0
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

        #
        # 有个中间数据型要先写入json
        sample_recommended = 'sample_recommended.json'
        # 这里我看x应该是可以写入default创建文件的，但执行的时候报错没有这个文件，新建一个应该就行问题不大，看你们要不要试一下直接存MySQL，这段我看不太懂
        with open(sample_recommended, 'x') as output_file:
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

        df_recommend_result = spark.read.json(sample_recommended)
        print("sample_recommended.json 临时文件表")
        df_recommend_result.show(20)
        print("df_recommend_result count: ")
        print(df_recommend_result.count())

        df_recommend_result.registerTempTable('recommend_result')
        # 这个df_final_recommend_result要存入MySQL(这条还没测试不清楚，因为上面那个存json的文件暂时不确定要不要这么写)
        df_final_recommend_result = spark.sql("SELECT DISTINCT b.user_id, a.ranks, c.name, c.header_image, c.steam_appid \
                                                   FROM recommend_result a, user_idx b, game_detail c \
                                                   WHERE a.user_idx = b.user_idx AND a.game_id = c.steam_appid \
                                                   ORDER BY b.user_id, a.ranks")
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
        df_final_recommend_result.write.jdbc(url=url, mode=mode, properties=df_global_popular_games_properties,
                                             table=final_recommend_result_table)

