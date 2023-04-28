import json
import time
import urllib.request
import re
import requests
from bs4 import BeautifulSoup as bs
from contextlib import closing

key = '8F8BBCEDF2B6E75EDC1F65A9DADB9A0E'


# step 1： Get userID
def get_user_id(user_profile, user_ids):
    url = user_profile

    with urllib.request.urlopen(url) as page:
        for line in page:
            if b"steamid" in line:
                try:
                    user_id = re.search(rb"\"steamid\":\"(\d+)\"", line).group(1).decode('utf-8')
                    print(user_id + ' ' + user_profile)
                    if user_id is not None:
                        user_ids.append(user_id)
                        break
                except Exception as e:
                    print(e)
                    continue


def get_online_users(member_list_no, user_ids):
    url = 'https://steamcommunity.com/games/steam/members?p=' + str(member_list_no)

    resp = requests.get(url)

    soup = bs(resp.text, 'html.parser')
    # print(soup.prettify())

    # search profile of users who are online/in-game
    all_users = soup.find_all("div",
                              onclick=re.compile("top\.location\.href='https:\/\/steamcommunity\.com\/id\/(\w+)'"),
                              class_=re.compile("online|in-game"))

    # get user names
    for user in all_users:
        user_profile = user.div.div.div.a['href']
        # print user_profile
        get_user_id(user_profile, user_ids)
        user_name = re.search('https:\/\/steamcommunity\.com\/id\/(\w+)', user_profile).group(1)


# step2: write id in file
def dump_user_id(user_ids, user_out_file):
    with open(user_out_file, 'w') as f:
        for idx in range(0, len(user_ids)):
            user_id_idx = {'user_idx': idx, 'user_id': user_ids[idx]}
            json.dump(user_id_idx, f)
            f.write('\n')


# step3: Get all games info
# get game id
def get_app_id_list():
    url = 'https://steamcommunity.com/linkfilter/https://api.steampowered.com/ISteamApps/GetAppList/v2/'
    header = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/57.0.2987.133 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2'}
    resp = requests.get(url, header)
    # [{"appid":1941401,"name":""}, ...]
    app_id_objs = resp.json()['applist']['apps']
    app_id_list = []

    for app in app_id_objs:
        app_id_list.append(app['appid'])

    return app_id_list


def get_game_detail(app_id_list, num, game_detail_out_file):
    url = 'https://store.steampowered.com/api/appdetails?appids='
    header = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/57.0.2987.133 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2'}
    with open(game_detail_out_file, 'w') as f:
        for idx in range(num):

            url_temp = url + str(app_id_list[idx])
            time.sleep(.100)  # sleep 100ms
            resp = requests.get(url_temp, header)

            obj = resp.json()
            print(obj)
            if obj is not None:
                for key in obj:
                    print(key)
                    if obj[key]["success"] is True:
                        json.dump(obj[key]["data"], f)
                        f.write('\n')
            else:
                print(idx)
                print(app_id_list[idx])
                print(obj)


def process_json_obj(resp, user_out_file, user_id):
    if 'user_summary' in user_out_file:
        # corner case: list index out of range
        try:
            obj = resp.json()['response']['players'][0]
        except Exception as e:
            obj = {'steamid': user_id}
            print(e)
    elif 'user_owned_games' in user_out_file:
        obj = resp.json()['response']
        obj = {'steamid': user_id, 'game_count': obj['game_count'], 'games': obj['games']}
    elif 'user_friend_list' in user_out_file:
        obj = resp.json()['friendslist']
        obj = {'steamid': user_id, 'friends': obj['friends']}
    elif 'user_recently_played_games' in user_out_file:
        obj = resp.json()['response']
        try:
            obj = {'steamid': user_id, 'total_count': obj['total_count'], 'games': obj['games']}
        except Exception as e:
            # corner case: total_count is zero
            print(e)
            obj = {'steamid': user_id, 'total_count': obj['total_count'], 'games': []}
    return obj


def dump_user_info(url, user_ids, user_out_file):
    with open(user_out_file, 'w') as f:
        for user_id in user_ids:
            url_temp = url + str(user_id)
            resp = requests.get(url_temp)
            # resp = requests.head(url_temp)
            obj = process_json_obj(resp, user_out_file, user_id)
            json.dump(obj, f)
            f.write('\n')
