# -*- coding: UTF-8 -*-
# Created by thpffcj on 2019/10/19.

import time
import pandas as pd

pd.set_option('display.max_columns', 40)
pd.set_option('display.width', 1000)


def generate_log(count=200000):

    data = pd.read_csv("/Users/thpffcj/Public/file/steam.csv")
    f = open("/Users/thpffcj/Public/file/user_data.log", "a")

    flag = 0
    position = 0
    while count >= 1:
        log = data.loc[position:position]
        query_log = "{user_id}\t{game_name}\t{behavior}\t{duration}".format(
            user_id=log["userId"].values.max(), game_name=log["gameName"].values.max(),
            behavior=log["behavior"].values.max(), duration=log["duration"].values.max())

        f.write(query_log + "\n")
        print(query_log)

        if flag % 500 == 0:
            time.sleep(5)

        count = count - 1
        position = position + 1


if __name__ == '__main__':
    generate_log()
