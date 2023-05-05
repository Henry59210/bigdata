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
    return json.dumps(obj)


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


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python main.py <user_id>")
    else:
        user_id = sys.argv[1]
        user_recent_played_games = get_user_recent_played_games(user_id)
        spark = SparkSession.builder.appName("games").getOrCreate()
        model = MatrixFactorizationModel.load(spark.sparkContext, 'als')
        df_user_recent_games = spark.read.json(user_recent_played_games)
        df_user_recent_games.registerTempTable("user_recent_games")
        df_valid_user_recent_games = spark.sql("SELECT b.user_idx, a.games FROM user_recent_games a \
                                                        JOIN user_idx b ON b.user_id = a.steamid WHERE a.total_count != 0")
        print("df_valid_user_recent_games")
        df_valid_user_recent_games.show(10)
        print("f_valid_user_recent_games count: ")
        print(df_valid_user_recent_games.count())

        # map and filter out the games whose playtime is 0
        testing_rdd = df_valid_user_recent_games.rdd.flatMapValues(lambda x: x) \
            .map(lambda x_y: (x_y[0], x_y[1].appid, x_y[1].playtime_forever)) \
            .filter(lambda x_y_z: x_y_z[2] > 0)
        testing_rdd.collect()
        print(model.predictAll(testing_rdd))