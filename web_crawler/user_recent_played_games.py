import json
import sys
import requests
from pyspark.mllib.recommendation import MatrixFactorizationModel
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS

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


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python main.py <user_id>")
    else:
        user_idx_file = 'user_idx_file.json'
        user_recent_games_file = 'user_recent_games_file.json'
        user_id = sys.argv[1]
        user_recent_played_games = get_user_recent_played_games(user_id)
        spark = SparkSession.builder.appName("games").getOrCreate()
        model = MatrixFactorizationModel.load(spark.sparkContext, '/home/azureuser/model/als')
        user_idx_str = '{"user_idx": 0, "user_id": ' + user_id + '}'
        dump_file(user_idx_file, user_idx_str)
        dump_file(user_recent_games_file, user_recent_played_games)
        df_user_idx = spark.read.json(user_idx_file)
        df_user_idx.registerTempTable("user_idx")
        df_user_idx.show()
        df_user_recent_games = spark.read.json(user_recent_games_file)
        df_user_recent_games.registerTempTable("user_recent_games")
        df_user_recent_games.show()
        df_valid_user_recent_games = spark.sql("SELECT b.user_idx, g.appid, g.playtime_forever \
                                                FROM user_recent_games a \
                                                JOIN user_idx b ON b.user_id = a.steamid \
                                                LATERAL VIEW explode(a.games) exploded_games AS g \
                                                WHERE a.total_count != 0") \
            .select("user_idx", "appid", "playtime_forever")

        print("df_valid_user_recent_games")
        df_valid_user_recent_games.show(10)
        print("f_valid_user_recent_games count: ")
        print(df_valid_user_recent_games.count())

        als = ALS(userCol='user_idx', itemCol='appid', ratingCol='playtime_forever')

        # map and filter out the games whose playtime is 0
        # testing_rdd = df_valid_user_recent_games.rdd.flatMapValues(lambda x: x) \
        #     .map(lambda x_y: (x_y[0], x_y[1].appid, x_y[1].playtime_forever)) \
        #     .filter(lambda x_y_z: x_y_z[2] > 0)
        # print(testing_rdd.collect())
        model1 = als.fit(df_valid_user_recent_games)

        user_id = '76561197965417975'
        user_idx_str = '{"user_idx": 1, "user_id": ' + user_id + '}'
        dump_file(user_idx_file, user_idx_str)
        dump_file(user_recent_games_file, user_recent_played_games)
        df_user_idx = spark.read.json(user_idx_file)
        df_user_idx.registerTempTable("user_idx")
        df_user_idx.show()
        df_user_recent_games = spark.read.json(user_recent_games_file)
        df_user_recent_games.registerTempTable("user_recent_games")
        df_user_recent_games.show()
        df_valid_user_recent_games = spark.sql("SELECT b.user_idx, g.appid, g.playtime_forever \
                                                        FROM user_recent_games a \
                                                        JOIN user_idx b ON b.user_id = a.steamid \
                                                        LATERAL VIEW explode(a.games) exploded_games AS g \
                                                        WHERE a.total_count != 0") \
            .select("user_idx", "appid", "playtime_forever")
        # Predict the ratings for the testing data using the loaded ALS model
        model1.transform(df_valid_user_recent_games)
        predictions = model1.recommendForAllUsers()
        print(predictions.collect())
