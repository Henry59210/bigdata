# This is a sample Python script.
from collections import defaultdict
from threading import Thread

import requests

from web_crawler.steam_data import get_online_users, dump_user_id, get_app_id_list, get_game_detail, dump_user_info

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

key = '8F8BBCEDF2B6E75EDC1F65A9DADB9A0E'
# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # step 1: get userID
    member_list_page_no = 300
    user_ids = []
    for idx in range(1, member_list_page_no + 1):
        print("Member List " + str(idx))
        get_online_users(idx, user_ids)
    print("Total online users found:")
    print(len(user_ids))

    # step 2: write id in file
    dump_user_id(user_ids, 'data/user_idx_sample.json')

    # step3: Get all games info
    app_id_list = get_app_id_list()
    print("total apps: " + str(len(app_id_list)))

    get_game_detail(app_id_list, 1000, "data/game_detail.json")

    # step 4: Get user related info

    # basic profile information for a list of 64-bit Steam IDs.
    url = 'http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key=' + key + '&steamids='
    dump_user_info(url, user_ids, 'data/user_summary_sample.json')

    # A list of games a player owns along with some playtime information,
    url = 'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key=' + key + '&steamid='
    dump_user_info(url, user_ids, 'data/user_owned_games_sample.json')

    # Friend list of any Steam user, provided their Steam Community profile visibility is set to "Public".
    url = 'http://api.steampowered.com/ISteamUser/GetFriendList/v0001/?key=' + key + '&steamid='
    dump_user_info(url, user_ids, 'data/user_friend_list_sample.json')

    # a list of games a player has played in the last two weeks
    url = 'http://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v0001/?key=' + key + '&steamid='
    dump_user_info(url, user_ids, 'data/user_recently_played_games_sample.json')
