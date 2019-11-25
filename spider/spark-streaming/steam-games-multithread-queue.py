import requests
from bs4 import BeautifulSoup
import pymongo
import re
import threading
from multiprocessing import JoinableQueue

#爬取中国区steam所有产品，存入mongodb中
def write(item):
    try:
        collection.insert_one(item)
    except:
        print(e)
        
def fetchReviewsChart(appID):
    url = "https://store.steampowered.com/appreviewhistogram/"+appID+"?l=schinese&review_score_preference=0"
    try: 
        res = session.get(url,headers=headers,timeout=30)
        if res.status_code != requests.codes.ok:#请求被拒绝打印出状态码,此页爬取失败
            print(res.status_code,":",url)
            return None
        chart = res.json()
        #chart.results.rollup_type: 取值有"week"、"month"，指的是chart.results.rollups中每个date的时间跨度
        #chart.results.recent 中每个date的时间跨度是1天
        #date: 时间
        #recommendations_down: 差评数
        #recommendations_up: 好评数
        return chart["results"] if chart["success"]==1 else None
    except: #网络有问题访问失败
        print(url)
        return None

def fetchGameInfo(url):
    try: 
        res = session.get(url,headers=headers,timeout=30)
        if res.status_code != requests.codes.ok:#请求被拒绝打印出状态码,此页爬取失败
            print(res.status_code,":",url)
            return None
    except: #网络有问题访问失败
        print(url)
        return None
        
    try: 
        soup = BeautifulSoup(res.text,'lxml')
        
        #社区的URL
        communityUrl = soup.find(class_="apphub_OtherSiteInfo").find("a").attrs["href"]
        appID = communityUrl.split("/")[-1]
        
        #右上角的概览
        user_reviews = soup.find(class_="user_reviews")
        user_reviews_json = {}
        for item in user_reviews.find_all("div",class_="subtitle column"):
            user_reviews_json[item.string.strip()] = re.sub('\r|\n|\t', '', item.parent.find_all("div")[1].get_text().strip())

        #用户自定义标签
        user_tags = soup.find(class_="glance_tags popular_tags")
        if user_tags:
            user_tags = [item.string.strip() for item in user_tags.find_all("a")]
        else:
            user_tags = []
        
        #该游戏支持的活动
        support_tags = soup.find_all(class_="game_area_details_specs")
        support_tags = [item.find(class_="name").get_text().strip() for item in support_tags]
        
        #爬取评论量图表
        reviewsChart = fetchReviewsChart(appID)
        if not reviewsChart: #失败重爬一次
            reviewsChart = fetchReviewsChart(appID)
        reviewsChart = reviewsChart if reviewsChart else ""

        #该页面的所有信息
        game_detail = {
            "user_reviews":user_reviews_json,
            "user_tags":user_tags,
            "support_tags":support_tags,
            "reviewsChart":reviewsChart,
        }
#         print(game_detail)
        return game_detail

    except: #steam服务器响应不正确
        print("bad url:",url)
        return None

class fetchThread(threading.Thread):
    def __init__(self, tqueue):
        threading.Thread.__init__(self)
        self.tqueue = tqueue
    def run(self):
        while True:
            info = self.tqueue.tasks.get()
            href = info["href"]
            if href.startswith("https://store.steampowered.com/bundle/") or href.startswith("https://store.steampowered.com/sub/"):
                game_detail = "bundle" #捆绑包不爬详情页
            else:
                game_detail = fetchGameInfo(href)
                if not game_detail: #失败重爬一次
                    game_detail = fetchGameInfo(href)
                game_detail = game_detail if game_detail else ""
            info["game_detail"] = game_detail

#             print(info)
            write(info)
            if game_detail=="":
                self.tqueue.addBadItem(info)
            self.tqueue.finishOne()
            self.tqueue.tasks.task_done() #已经处理完从队列中拿走的一个项目

class threadQueue:
    def __init__(self, numOfThreads):
        self.numOfThreads = numOfThreads
        self.threads = []
        self.tasks = JoinableQueue()#实例一个队列
        self.tasksNum = 0
        self.badItems = []
        
        for i in range(1,numOfThreads):
            # 创建线程爬取详情页面
            thread = fetchThread(self)
            thread.start()
            self.threads.append(thread)
    def add(self,info):
        self.tasks.put(info)
    def finishOne(self):
        threadLock = threading.Lock()
        threadLock.acquire()
        self.tasksNum=self.tasksNum+1
        if self.tasksNum%25==0:
            print(self.tasksNum,"/",(totalPage-1)*25,"finished")
        threadLock.release()
    def addBadItem(self,info):
        self.badItems.append(info)
    def waitForStop(self):
        self.tasks.join()#等,直到消费者把自己放入队列中的所有项目都取走处理完后调用task_done()之后
        if self.badItems:
            print("badItems ",self.badItems) 
        
def fetch(pageRange):
    badPages = []
    page =1 #每页一个request
    for page in pageRange:
        try: #网络有问题访问失败，保存失败的请求然后跳过
            url = "https://store.steampowered.com/search/?page=" + str(page)
            res = session.get(url,headers=headers)
        except:
            badPages.append(page)
            continue

        if res.status_code != requests.codes.ok:#请求被拒绝打印出状态码然后跳过
            print("page",page,":",res.status_code)
            badPages.append(page)
            continue

        try: #曾出现过异常，当时没仔细看，但是后面都没再出现了，可能与steam的服务器有关
            soup = BeautifulSoup(res.text,'lxml')
            contents = soup.find(id="search_resultsRows").find_all('a')
        except:
            print("bad page:",page)
            badPages.append(page)
            continue

        for content in contents:
            try:
                name = content.find(class_="title").get_text().strip()
                date = content.find("div",class_="col search_released responsive_secondrow").string
                date = date.strip() if date else ""#未上市的没有发行日期
                priceDiv=content.find("div",class_="col search_price discounted responsive_secondrow")
                if priceDiv:#打折游戏
                    original_price=priceDiv.find("strike").string.strip()
                    price=priceDiv.contents[-1].strip()
                else:#原价游戏
                    original_price= content.find("div",class_="col search_price responsive_secondrow").string.strip()
                    price=original_price
                img_src = content.find("div",class_="col search_capsule").find('img').get("src")
                href = content.get("href")
                review_summary = content.find("span",class_="search_review_summary")
                review_summary = review_summary.attrs['data-tooltip-html'].strip() if review_summary else ""#未上市的没有总评
                result={
                    "page":page,
                    "name":name,
                    "href":href,
                    "date":date,
                    "original_price":original_price,
                    "price":price,
                    "img_src":img_src,
                    "review_summary":review_summary,
                }
                queue.add(result)
            except:
                print(content)
    queue.waitForStop()
    return badPages

#mongodb连接
client = pymongo.MongoClient('mongodb://steam:steam@***.***.***.***:27017/steam_db')
db = client.steam_db
regions = db.China
collection = regions.games

#建立会话，Cookie设置语言为简体中文,出生日期为1987.1.1（允许访问成人内容）
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36',
    'Cookie':'Steam_Language=schinese; birthtime=533750401'
}
session = requests.session()

queue = threadQueue(100)

totalPage = 2 #目前有2608页
badPages = fetch(range(1, totalPage))
if badPages: #重爬坏页
    badPages = fetch(badPages)
print("all finished")
if badPages:
    print("badPages:",badPages)
