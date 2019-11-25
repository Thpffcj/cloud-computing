import requests
from bs4 import BeautifulSoup
import pymongo

#爬取中国区steam当前热销榜，存入mongodb中
def write(item):
    try:
        collection.insert_one(item)
    except:
        print(e)

def fetch(pageRange):
    badPages = []
    badItems = []
    page =1 #每页一个request
    for page in pageRange:
        try: #网络有问题访问失败，保存失败的请求然后跳过
            url = "https://store.steampowered.com/search/?filter=globaltopsellers&page=" + str(page) + "&os=win"
            #Cookie设置语言为简体中文
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36',
                'Cookie':'Steam_Language=schinese'
            }
            s = requests.session()
            res = s.get(url,headers=headers)
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
            print(soup)
            badPages.append(page)

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
                review_summary = review_summary.attrs['data-tooltip-html'].strip() if review_summary else ""#未上市的没有发行日期
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
#                 print(result)
                write(result)
            except:
                print(content)
                badItems.append(content)
        if page%10==0:
            print(page,"/",totalPage,"finished")#每10页打印一次进度
    print("badItems:",badItems)
    return badPages

client = pymongo.MongoClient('mongodb://steam:steam@***.***.***.***:27017/steam_db')
db = client.steam_db
regions = db.China
collection = regions.hot

totalPage = 593 #目前有593页
badPages = fetch(range(1, totalPage))
if badPages: #重爬坏页
    badPages = fetch(badPages)
print("all finished")
if badPages:
    print("badPages:",badPages)
