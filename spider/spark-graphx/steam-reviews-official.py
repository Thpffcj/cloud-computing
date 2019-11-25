import requests
from bs4 import BeautifulSoup
import pymongo
import re
import threading
from multiprocessing import JoinableQueue
import time

#爬取steam所有评论，存入mongodb中
def write(item):
    try:
        if isinstance(item,list):
            collection.insert_many(item)
        else:
            collection.insert_one(item)
    except Exception as e:
        print(e)
    return True
        
def getAllApps():
    try:
        apps = []
        for g in regions.game_id.find().skip(0).limit(1400):
            apps.append({"id":g["id"],"name":g["name"]})
        return apps
    except:
        print(e)
        
def fetchReview(url,params,headers,app):
    try: 
        res = session.get(url,params=params,headers=headers,timeout=30,verify=False)
        if res.status_code != requests.codes.ok:#请求被拒绝打印出状态码,此页爬取失败
            if res.status_code != requests.codes.forbidden and res.status_code != requests.codes.bad_gateway: #403、502不打印
                print(res.status_code,":",url)
            return None
    except Exception as e: #网络有问题访问失败
        print(e)
        return None
    
    result = res.json()
#     print(res.url)
#     print(result)
    reviews = result["reviews"]
    if not reviews:#该游戏没有更多评论了
        print(result)
        return None
    cursor = result["cursor"]
    if not cursor:
        print(result)
    for review in reviews:
        review["game"] = app
    write(reviews)
#     print(url)
#     print(reviews)
#     print()
    return cursor
        
def fetch(apps):
    for app in apps:
        #建立会话，Cookie设置语言为简体中文,出生日期为1987.1.1（允许访问成人内容）
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36',
            'Cookie':'Steam_Language=schinese; birthtime=533750401; timezoneOffset=28800,0;',
        }
        cursor = "*"
        reviewsCount = 0
        while cursor:
#             print("cursor:",cursor)
#             url = "https://store.steampowered.com/appreviews/"+app["id"]+"?json=1&filter=recent&language=schinese&day_range=360"+ \
#             "&cursor="+cursor+"&review_type=all&purchase_type=all&num_per_page=100"
            url = "https://store.steampowered.com/appreviews/"+app["id"]
            params = {
                "json":1,
                "filter":"recent", #all,recent,updated 
                "language":"schinese", #all,schinese,zh-CN
                "day_range":"360",
                "cursor":cursor,
                "review_type":"all",
                "purchase_type":"all",
                "num_per_page":100,
            }
            cursor = fetchReview(url,params,headers,app)
            reviewsCount = reviewsCount+100
            if reviewsCount>=10000:
                break
        print(url,reviewsCount)

#mongodb连接
client = pymongo.MongoClient('mongodb://steam:steam@***.***.***.***:27017/steam_db')
db = client.steam_db
regions = db.China
collection = regions.reviews_official

requests.packages.urllib3.disable_warnings()
session = requests.session()

appInfos = getAllApps()
# print(appInfos)
numOfThreads = 1
badPages = fetch(appInfos)
print("all finished")

# https://store.steampowered.com/appreviews/243470?json=1&filter=all&language=all&day_range=360&cursor=*&review_type=all&purchase_type=all&num_per_page=10