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
    member_list_page_no = 1
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

# 多线程版本
# pages_to_iterate = 400
# concurrency = 10
# # each thread gets its own "user_ids_mt" list
# user_ids_mt = defaultdict(list)
#
#
# def get_online_users_wrapper(lower_bound, upper_bound, user_ids_local):
#     for idx in range(pages_lower_bound, pages_upper_bound):
#         get_online_users(idx, user_ids_local)
#
#
# threadlist = []
# for thread_id in range(concurrency):
#     user_ids_local = user_ids_mt[thread_id]
#     pages_lower_bound = pages_to_iterate / concurrency * thread_id + 1
#     pages_upper_bound = pages_to_iterate / concurrency + pages_lower_bound
#     # print pages_lower_bound
#     # print pages_upper_bound
#     thread = Thread(target=get_online_users_wrapper, args=(pages_lower_bound, pages_upper_bound, user_ids_local,))
#     thread.start()
#     threadlist.append(thread)
#
# for thread in threadlist:
#     thread.join()
# user_ids_flatten = []
# for concur in range(concurrency):
#     user_ids_flatten.extend(user_ids_mt[concur])
#
# print("Total users found in the first " + str(pages_to_iterate) + " pages of online member list:")
# print(len(user_ids_flatten))
#
# dump_user_id(user_ids_flatten, 'user_idx_full.json')
