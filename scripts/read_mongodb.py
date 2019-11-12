# -*- coding: UTF-8 -*-
# Created by thpffcj on 2019/10/30.

import pymongo


# 连接数据库
client1 = pymongo.MongoClient("101.132.176.87", 27017)

db1 = client1['steam_db']
db1.authenticate("steam", "steam")

table = db1['China.reviews_official']

data = table.find().limit(300000)
print("数据加载完成...")
# 65175
# for d in data:
#     print(d["game"])


# Python写MongoDB
client2 = pymongo.MongoClient("127.0.0.1", 27017)
# 库名inventory
db2 = client2['test']
# 集合名items
collection = db2['China.reviews_official_30W']

# 插入一个文档，item是一个字典{}
collection.insert(data)