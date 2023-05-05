from steam_data import get_game_detail, get_app_id_list

if __name__ == '__main__':
    # 游戏信息获取
    # step3: Get all games info
    app_id_list = get_app_id_list()
    print("total apps: " + str(len(app_id_list)))

    game_detail_content = []
    get_game_detail(app_id_list, 1000, "data/game_detail.json", game_detail_content)