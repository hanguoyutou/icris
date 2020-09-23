#-*- coding:utf-8 –*-
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from ssl import SSLEOFError
import csv
import sqlite3
import time
import logging
import os
import requests
'''
该类执行
1. 在google搜索对应【人+公司名+keyword】或者【公司名+keyword】，来获取新闻链接。
2. 将所获取到的链接去重
3. 将去重的链接保存到数据库
'''
GOOGLE_PAGE = 3
class search():
    keywords = []
    names = []
    compname = ""
    name_keyword = dict()
    links = list()
    no_record = "null"
    MONTH = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    account_db = "/root/NegNews/predeploy/data/static/account.db"

    developerKey = ""

    cx = ""

    def __init__(self,logger):
        self.logger = logger

    def google_search(self,search_term, **kwargs):
        self.get_gcs_account()
        developerKey=self.developerKey
        cx=self.cx
        url = 'https://www.googleapis.com/customsearch/v1?key='+developerKey+'&cx=' + cx + '&num=10'+'&q='+ search_term
        res = requests.get(url).json()
        print (res)
        # service = build("customsearch", "v1",developerKey=self.developerKey)
        # res = service.cse().list(q=search_term, cx=self.cx, **kwargs).execute()
        return res

    def meta_data_import(self, function=None, keyword_f=None, namefile=None, compname=None, personname=None):
        keywords_byte = csv.reader(open(keyword_f,'r'))
        for keyword in keywords_byte:
            self.keywords.append(keyword[0])

        # for batch comp/person name nagetive search, only for testing and preproduction period
        if function == "batch":
            names_byte = csv.reader(open(namefile, "r"), delimiter='$')
            for name in names_byte:
                self.names.append(name[0].rstrip(','))
        # for company nagetive news search
        elif function == "single_comp":
            self.names.append(compname)
        # for person nagetive news search which also needs company name as a search parameter
        elif function == "single_person":
            self.names.append(personname)
            self.compname = compname
        elif function == "retail" and personname != None:
            self.names.append(personname)

        for name in self.names:
            self.name_keyword[name]=list()
            for keyword in self.keywords:
                self.name_keyword[name].append(keyword)

    def crawl_googleapi(self, function=None, fileStorage=None):
        # 预查询已经爬取的name+keyword
        exist_name_key = dict()
        self.links = list()
        retry = 3
        if function == "batch":
            if not os.path.exists(fileStorage):
                self.create_db(fileStorage)
            else:
                conn = sqlite3.connect(fileStorage)
                c = conn.cursor()
                self.logger.info("数据库打开成功")
                cursor = c.execute("SELECT * FROM tb_links")
                for row in cursor:
                    # row[1]是compname row[2]是keyword
                    if row[1] in exist_name_key:
                        exist_name_key[row[1]].append(row[2])
                    else:
                        exist_name_key[row[1]] = [row[2]]
                conn.close()
            error_rate = self.crawler_scheduler(exist_name_key=exist_name_key,db_f=fileStorage)
            self.logger.info("error_rate is:" + str(error_rate))
        else :
            # 检查文件数据库是否存在，存在就删除再重新创建，保持每次调用该方法的结果是一致的，就是在新的数据库进行存取
            # 这样可以保证同一个公司爬取多少次都可以,bug24
            if not os.path.exists(fileStorage):
                while retry != 0:
                    self.create_db(fileStorage)
                    error_rate = self.crawler_scheduler(exist_name_key=exist_name_key, db_f=fileStorage)
                    if error_rate < 0.5:
                        self.logger.info("error_rate is:"+str(error_rate))
                        return {"result": "succ"}
                    else:
                        # delete the database and
                        os.remove(fileStorage)
                        retry -= 1
                # if get bad result 3 times
                return {"result": "fail"}
            else:
                os.remove(fileStorage)
                # return {"result": "existed"}
                while retry != 0:
                    self.create_db(fileStorage)
                    error_rate = self.crawler_scheduler(exist_name_key=exist_name_key, db_f=fileStorage)
                    if error_rate < 0.5:
                        self.logger.info("error_rate is:"+str(error_rate))
                        return {"result": "succ"}
                    else:
                        # delete the database and
                        os.remove(fileStorage)
                        retry -= 1
                # if get bad result 3 times
                return {"result": "fail"}

    def crawler_scheduler(self, exist_name_key=None, db_f=None):
        total_page = 0
        error_page = 0
        for name in self.name_keyword:
            self.logger.info("=============================================")
            self.logger.info(time.strftime("%H:%M:%S", time.localtime()))
            # {compname:{keyword:{link:title,link:title},keyword:{link:title,link:title}}}
            # comp_links = {name:{}}
            # if '&' not in compname:
            # for keyword in self.name_keyword[name]:
            query = self.gen_query(self.name_keyword)
            self.logger.info("********************************************")
            # if name in exist_name_key and keyword in exist_name_key[name]:
            #     self.logger.info("该名称:"+name+"和keyword:"+keyword+"已经爬取")
            # else:
                # link+title的dict
                # comp_links[name][keyword] = {}
                # 爬三页
            start = 0
            self.logger.info("正在爬：name:"+name)
            for i in range(0,GOOGLE_PAGE):
                total_page += 1
                result = self.crawler_worker(name=name,keyword=query,page=i,start=start,db_f=db_f)
                time.sleep(6)
                if result["result"] == "empty":
                    break
                elif result["result"] == "fail":
                    error_page += 1
                else:
                    start = result["result"]
            self.logger.info("名字:" + name + "　一共有" + str(len(self.links)) + "links")
        return error_page/total_page

    def crawler_worker(self,name=None,keyword=None,page=None,start=None,db_f=None):
        try:
            res = self.crawler(name=name,keyword=keyword,page=page,start=start)
            # 解析res，首先看res是否是报错，如果报错说明没有这一页，则跳出底层for循环。否则提取res中的link,保存到sqlite
            # 因为是第一页，如果没有记录说明该keyword没有记录，但是也要记录到数据库，这样下次退出再爬取的时候才知道不需要再爬了
            if res['queries']['request'][0]['totalResults'] == '0':
                self.logger.info("该keyword没有记录，爬取下一个关键词")
                data = {"name": name, "keyword": keyword, "link": 'Null', "title": 'Null', "pub_date": 'Null'}
                self.save(data=data, db_f=db_f, comp_type=self.no_record)
                return {"result":"empty"}
            else:
                self.logger.info("该页一共" + str(len(res['items'])) + "个记录")
                if len(res['items']) < 10:
                    for item in res['items']:
                        if item['link'] not in self.links:
                            self.logger.info("保存新link")
                            self.links.append(item["link"])
                            date = self.date_extract(item=item)
                            data = {"name": name, "keyword": keyword, "link": item['link'], "title": item['snippet'],
                                    "pub_date": date}
                            self.save(data=data, db_f=db_f)
                    self.logger.info("该keyword已经没有记录，爬取下一个关键词")
                    return {"result": "empty"}
                else:
                    for item in res['items']:
                        if item['link'] not in self.links:
                            self.logger.info("保存新link")
                            self.links.append(item['link'])
                            date = self.date_extract(item=item)
                            data = {"name": name, "keyword": keyword, "link": item['link'], "title": item['snippet'],
                                    "pub_date": date}
                            self.save(data=data, db_f=db_f)
                    return {"result":start+10}
        except HttpError as e:
            self.logger.exception(e)
            return {"result": "fail"}
        except SSLEOFError as e:
            self.logger.exception(e)
            return {"result": "fail"}
        except Exception as e:
            self.logger.exception(e)
            return {"result": "fail"}

    def crawler(self,name=None,keyword=None,page=None,start=None):
        res = None
        if start == 0:
            # 如果keyword带空格，需要加引号
            # if self.compname != "" and " " in keyword:
            #     self.logger.info("多单词keyword第" + str(page + 1) + "页")
            #     res = self.google_search("\"" + self.compname + "\" " + "\"" + name + "\" " + "\"" + keyword + "\"",
            #                              num=10)
            # elif self.compname == "" and " " in keyword:
            #     self.logger.info("多单词keyword第" + str(page + 1) + "页")
            #     res = self.google_search("\"" + name + "\" " + "\"" + keyword + "\"", num=10)
            if self.compname != "":
                self.logger.info("所有keywords第" + str(page + 1) + "页")
                # res = self.google_search('+"' + self.compname + '"+' + '"' + name + '"+(' + keyword +')', num=10)
                res = self.google_search('"' +self.compname + '"+' + '"' + name + '"+(' + keyword +')')
            else:
                self.logger.info("所有keywords第" + str(page + 1) + "页")
                # res = self.google_search('+"' + name + '"+(' + keyword +')', num=10)
                res = self.google_search('"' +name + '"+(' + keyword +')')
        else:
            # if self.compname != "" and " " in keyword:
            #     self.logger.info("多单词keyword第" + str(page + 1) + "页")
            #     res = self.google_search("\"" + self.compname + "\" " + "\"" + name + "\" " + "\"" + keyword + "\"",
            #                              num=10, start=start)
            # elif self.compname == "" and " " in keyword:
            #     self.logger.info("多单词keyword第" + str(page + 1) + "页")
            #     res = self.google_search("\"" + name + "\" " + "\"" + keyword + "\"", num=10, start=start)
            if self.compname != "":
                self.logger.info("所有keywords第" + str(page + 1) + "页")
                # res = self.google_search('+"' + self.compname + '"+' + '"' + name + '"+(' + keyword +')', num=10,
                #                          start=start)
                res = self.google_search('"' +self.compname + '"+' + '"' + name + '"+(' + keyword +')'+ '&start='+ str(start))
            else:
                self.logger.info("所有keywords第" + str(page + 1) + "页")
                # res = self.google_search('+"' + name + '"+(' + keyword +')', num=10, start=start)
                res = self.google_search('"' + name + '"+(' + keyword +')' + '&start='+ str(start))
        return res

    def save(self,data=None,db_f=None,comp_type=None):
        sql = ""
        try:
            name = data["name"]
            keyword = data["keyword"]
            link = data["link"]
            title = data["title"]
            news_date = data["pub_date"]
            conn = sqlite3.connect(db_f)
            c = conn.cursor()
            self.logger.info("数据库打开成功")
            title_convert = title.replace('"', '')
            # bug14，允许人名中输入",等符号
            if comp_type == self.no_record:
                sql = "INSERT INTO tb_links (name,keyword,link,title,if_crawled,if_nered,create_time,news_time) VALUES " \
                      "(\'" + name + "\',\'all\',\'" + link + "\',\'" + title_convert + "\',\'Null\',\'Null\',\'" + \
                        str(time.strftime("%a %b %d %H:%M:%S %Y", time.localtime())) + "\',\'" + news_date + "\')"
                c.execute(sql)
            else:
                sql = "INSERT INTO tb_links (name,keyword,link,title,if_crawled,if_nered,create_time,news_time) VALUES " \
                      "(\'" + name + "\',\'all\',\'" +link + "\',\'" + title_convert + "\',\'False\',\'False\',\'" + \
                      str(time.strftime("%a %b %d %H:%M:%S %Y", time.localtime())) + "\',\'" + news_date + "\')"
                c.execute(sql)
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error("===================")
            self.logger.error("======ERROR========")
            self.logger.error(sql)
            self.logger.error("===================")
            self.logger.exception(e)

    def date_extract(self,item):
        title = item['snippet']
        date = ""
        try:
            for key in item["pagemap"]["metatags"][0]:
                if "published" in key or "issued" in key:
                    date = item["pagemap"]["metatags"][0][key]
                    break
            if date == "":
                maybe_date = title[0:12]
                for i in self.MONTH:
                    if i in maybe_date:
                        date = maybe_date
                        break
        except Exception as e:
            logging.error(e)
        return date

    # 对location中有特殊字符进行处理bug14
    def create_db(self,location):
        conn = sqlite3.connect(location)
        c = conn.cursor()
        c.execute('''CREATE TABLE tb_links (
            id	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            name	TEXT NOT NULL,
            keyword	TEXT NOT NULL,
            link	TEXT NOT NULL,
            title	TEXT NOT NULL,
            if_crawled	TEXT NOT NULL,
            if_nered	TEXT NOT NULL,
            create_time	TEXT NOT NULL,
            news_time	TEXT NOT NULL
        );''')
        conn.commit()
        conn.close()

    # TODO 当前只增加数据库存取账号密码功能，等到做多账户需求时和流量控制需求时，再增加对于quata和status的控制
    def get_gcs_account(self):
        self.logger.info(self.account_db)
        conn = sqlite3.connect(self.account_db)
        cursor = conn.execute("SELECT key, cx FROM gcs WHERE status=0")
        records = list(cursor)
        if len(records) != 0:
            self.developerKey = records[0][0]
            self.cx = records[0][1]
            # conn.execute("UPDATE icris SET status = 1 WHERE username='"+self.account+"'")
            # conn.commit()
            conn.close()
            return True
        else:
            conn.close()
            return False

    def gen_query(self,name_keywords):
        query = ""
        for name in name_keywords:
            for keyword in name_keywords[name]:
                query += '"' + keyword + '" OR '
            query = query[0:-4]
        return query
