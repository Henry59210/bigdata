# This is a sample Python script.
from collections import defaultdict
from threading import Thread

import requests
from kafka_utils import producer

from web_crawler.steam_data import get_users, dump_user_id, get_app_id_list, get_game_detail, dump_user_info

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

key = '8F8BBCEDF2B6E75EDC1F65A9DADB9A0E'
# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # step 1: get userID
    member_list_page_no = 500
    user_ids = []
    for idx in range(1, member_list_page_no + 1):
        print("Member List " + str(idx))
        get_users(idx, user_ids)
    print("Total online users found:")
    print(len(user_ids))

    # step 2: write id in file
    user_id_content = []
    dump_user_id(user_ids, 'data/user_idx_sample.json', user_id_content)
    print(user_id_content)
    # producer.push_message('user_idx', user_id_content)



    # 游戏信息获取
    # step3: Get all games info
    # app_id_list = get_app_id_list()
    # print("total apps: " + str(len(app_id_list)))

    # game_detail_content = []
    # get_game_detail(app_id_list, 1000, "data/game_detail.json", game_detail_content)

    # step 4: Get user related info

    # basic profile information for a list of 64-bit Steam IDs.
    url = 'http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key=' + key + '&steamids='
    user_summary = dump_user_info('user_summary', url, user_ids, 'data/user_summary_sample.json')

    # A list of games a player owns along with some playtime information,
    url = 'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key=' + key + '&steamid='
    user_owned_games = dump_user_info('user_owned_games', url, user_ids, 'data/user_owned_games_sample.json')

    # Friend list of any Steam user, provided their Steam Community profile visibility is set to "Public".
    url = 'http://api.steampowered.com/ISteamUser/GetFriendList/v0001/?key=' + key + '&steamid='
    user_friend_list = dump_user_info('user_friend_list', url, user_ids, 'data/user_friend_list_sample.json')

    # a list of games a player has played in the last two weeks
    url = 'http://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v0001/?key=' + key + '&steamid='

    user_recently_played_games = dump_user_info('user_recently_played_games', url, user_ids, 'data/user_recently_played_games_sample.json')
