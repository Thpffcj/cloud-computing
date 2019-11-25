import requests
from bs4 import BeautifulSoup
import pymongo
import re
import threading
from multiprocessing import JoinableQueue
import time

# 3 1        1400           678960,1122050,1100620,730,1041320
#爬取中国区steam所有评论，存入mongodb中
def write(item):
    try:
        if isinstance(item,list):
#             firstUser = regions.first_review_user.find({"game":item[0]["game"]["id"]})[0]
#             if firstUser["user"]==item[0]["user"]["name"]: #重复评论
#                 return False
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
            apps.append({"id":g["id"],"name":g["name"],"firstUser":None})
        return apps
    except:
        print(e)
        
def fetchReview(url,headers,app):
    try: 
        res = session.get(url,headers=headers,timeout=30,verify=False)
        if res.status_code != requests.codes.ok:#请求被拒绝打印出状态码,此页爬取失败
            if res.status_code != requests.codes.forbidden and res.status_code != requests.codes.bad_gateway: #403、502不打印
                print(res.status_code,":",url)
            return None
    except Exception as e: #网络有问题访问失败
        print(e)
        return None
    
    if not res.text:#该游戏没有更多评论了
        return None
        
    try: 
        soup = BeautifulSoup(res.text,'lxml')
        
        reviewGroup = []
        for card in soup.find_all(class_="apphub_Card modalContentLink interactable"):
            userCard = card.find(class_="apphub_friend_block")
            if not userCard:# 没有用户的评论扔掉
                continue
            if not userCard.find(class_="apphub_CardContentAuthorName"):# 没有用户的评论扔掉
                continue
            if(len(userCard.find(class_="apphub_CardContentAuthorName").find_all("a"))!=1):
                print(userCard.find(class_="apphub_CardContentAuthorName"))
            name = userCard.find(class_="apphub_CardContentAuthorName").find("a").string
            name = name.strip() if name else ""
            product_owns = userCard.find(class_="apphub_CardContentMoreLink").string
            product_owns = product_owns.strip() if product_owns else ""
            user = {
                "name":name,# 可能为""
                "product_owns": product_owns,# 可能为""
            }
            comment_count = card.find(class_="apphub_CardCommentCount").string.strip()
            found_helpful = card.find(class_="found_helpful").contents
            helpful_num = found_helpful[0].strip()
            funny_num = found_helpful[-1].strip() if len(found_helpful)>1 else ""
            title = card.find(class_="reviewInfo").find(class_="title").string.strip()
            hours = card.find(class_="reviewInfo").find(class_="hours")
            hours = hours.string.strip() if hours else ""
            
            cardTextContent = card.find(class_="apphub_CardTextContent")
            date_posted = cardTextContent.find(class_="date_posted").string.strip()
            content = cardTextContent.contents[5:] if cardTextContent.find(class_="received_compensation") else cardTextContent.contents[2:]
            content = "".join(item.string if item.string else "<br>" for item in content).strip()
            
            review = {
                "game":app,
                "user":user,
                "comment_count":comment_count,#该评论回复数
                "helpful_num":helpful_num,#几人觉得这篇评测有价值 有的是一句话，有的是数字
                "funny_num":funny_num,#几人觉得这篇评测欢乐
                "title":title,#推荐/不推荐
                "hours":hours,#总时数 可能为""
                "date_posted":date_posted,#发布于
                "content":content,#评论内容
            }
            reviewGroup.append(review)
        form = soup.find("form")
        nextUrl = form.attrs["action"]+"?"
        for arg in form.find_all("input"):
            nextUrl = nextUrl+arg.attrs["name"]+"="+arg.attrs["value"]+"&"
        nextUrl = nextUrl[:-1]
#         if app["firstUser"]==reviewGroup[0]["user"]["name"]:
#             return None
#         write(reviewGroup)
        print(url)
        print(reviewGroup)
        print()
#         print("nextUrl",nextUrl)
        return nextUrl

    except Exception as e: #steam服务器响应不正确
        print("bad url:",url,e)
        return None

class fetchThread(threading.Thread):
    def __init__(self, tQueue, app, threadNum):
        threading.Thread.__init__(self)
        self.tQueue = tQueue
        self.app = app
        self.threadNum = threadNum
    def run(self):
        id = self.app["id"]
        p = str(self.threadNum+1)
        userreviewsoffset = str((int(p)-1)*10)
        numperpage = "10"
#         url = "https://steamcommunity.com/app/"+id+"/homecontent/?userreviewsoffset="+userreviewsoffset+"&p="+p+ \
#         "&workshopitemspage="+p+"&readytouseitemspage="+p+"&mtxitemspage="+p+"&itemspage="+p+"&screenshotspage="+p+ \
#         "&videospage="+p+"&artpage="+p+"&allguidepage="+p+"&webguidepage="+p+"&integratedguidepage="+p+ \
#         "&discussionspage="+p+"&numperpage="+numperpage+"&browsefilter=trendyear&browsefilter=trendyear&l=schinese"+ \
#         "&appHubSubSection="+numperpage+"&filterLanguage=default&searchText=&forceanon=1"
        url = "https://steamcommunity.com/app/"+id+"/reviews/?p=1&browsefilter=trendyear"
        #建立会话，Cookie设置语言为简体中文,出生日期为1987.1.1（允许访问成人内容）
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36',
            'accept':'text/javascript, text/html, application/xml, text/xml, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh,zh-CN;q=0.9,zh-TW;q=0.8,en;q=0.7,en-GB;q=0.6,en-US;q=0.5',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'Cookie':'Steam_Language=schinese; birthtime=533750401; timezoneOffset=28800,0; sessionid=04a0dcb8f1f8f31bed482819; recentlyVisitedAppHubs=816340%2C678960%2C242920%2C1122050; steamCountry=CN%7C72e4ed8aa9f1f07b0eeba82d9349680e; app_impressions=1122050@2_9_100010_|1122050@2_9_100010_|1122050@2_9_100010_|1122050@2_9_100010_|1122050@2_9_100010_|1122050@2_9_100010_|1122050@2_9_100010_|1122050@2_9_100010_|1122050@2_9_100010_|1122050@2_9_100010_|1122050@2_9_100010_',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'x-prototype-version': '1.7',
            'x-requested-with': 'XMLHttpRequest',
            'referer': url
        }
#         print(url)
        reviewCount = 0
        while url:
#             print("threadNum:"+str(self.threadNum)+" offset:"+userreviewsoffset)
            nextUrl = fetchReview(url,headers,self.app)
            if not nextUrl:
                nextUrl = fetchReview(url,headers,self.app)
                if not nextUrl: #2次失败认为这个游戏评论已爬完
                    break
            url = nextUrl
#             if not self.app["firstUser"]:
#                 self.app["firstUser"]=reviewGroup[0]["user"]["name"]
            reviewCount = reviewCount+10
            if int(reviewCount)>5000:#超过5K条评论后面的就不爬了（以魂3评论量为基准）
                break
#             p = str(int(p)+self.tQueue.numOfThreads*1)
#             userreviewsoffset = str((int(p)-1)*10)
#             url = "https://steamcommunity.com/app/"+id+"/homecontent/?userreviewsoffset="+userreviewsoffset+"&p="+p+ \
#             "&workshopitemspage="+p+"&readytouseitemspage="+p+"&mtxitemspage="+p+"&itemspage="+p+"&screenshotspage="+p+ \
#             "&videospage="+p+"&artpage="+p+"&allguidepage="+p+"&webguidepage="+p+"&integratedguidepage="+p+ \
#             "&discussionspage="+p+"&numperpage="+numperpage+"&browsefilter=toprated&browsefilter=toprated&l=schinese"+ \
#             "&appHubSubSection="+numperpage+"&filterLanguage=default&searchText=&forceanon=1"
#             trendyear toprated trendweek trendday mostrecent
#             print("nextUrl",url)
#             time.sleep(2)
#             break

class threadQueue:
    def __init__(self, numOfThreads, app):
        self.numOfThreads = numOfThreads
        self.app = app
        self.threads = []
        self.badItems = []
        
        for i in range(0,numOfThreads):
            # 创建线程爬取详情页面
            thread = fetchThread(self,app,i)
            thread.start()
            self.threads.append(thread)
#     def addBadItem(self,info):
#         self.badItems.append(info)
    def waitForStop(self):
        #等待当前页的线程爬取完后再开始爬下一页
        for t in self.threads:
            t.join()
        if self.badItems:
            print("badItems ",self.badItems) 
        
def fetch(apps):
    for app in apps:
        queue = threadQueue(numOfThreads,app)
        queue.waitForStop()
        print(app["id"],"finished")
    badItems = queue.badItems
    
    #错页重爬
    for app in badItems:
        queue = threadQueue(numOfThreads,app)
        queue.waitForStop()
    return queue.badItems

#mongodb连接
client = pymongo.MongoClient('mongodb://steam:steam@***.***.***.***:27017/steam_db')
db = client.steam_db
regions = db.China
collection = regions.reviews

requests.packages.urllib3.disable_warnings()
session = requests.session()

appInfos = getAllApps()
# print(appInfos)
numOfThreads = 1
badPages = fetch(appInfos)
print("all finished")

# http://store.steampowered.com/appreviews/243470?json=1&filter=all&language=all&day_range=360&cursor=*&review_type=all&purchase_type=all&num_per_page=10