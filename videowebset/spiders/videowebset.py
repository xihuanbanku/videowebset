#!/usr/bin/env python
# coding=utf-8
from __future__ import absolute_import

import json
import re
import time

import psycopg2
import scrapy
import tldextract

from videowebset.settings import DATABASE


class VideowebsetSpider(scrapy.Spider):
    name = "videowebset"
    db = psycopg2.connect(database=DATABASE['database'], user=DATABASE['user'], password=DATABASE['password'], host=DATABASE['ip'], port=DATABASE['port'])
    cur = db.cursor()
    type_dict = {u"电影":1,u"电视剧":2,u'剧集':2}    #获取URL地址
    keylist = []

    def start_requests(self):
        url_funcs = {
            "iqiyi.com":self.parse_iqiyi,
            "qq.com":self.parse_tencent,
            "youku.com":self.parse_youku,
            "sohu.com":self.parse_sohu,
            "le.com":self.parse_letv,
            "wasu.cn":self.parse_wasu,
            "cntv.cn":self.parse_cntv,
            "cctv.com":self.parse_cntv
        }
        content = self.getDBdata()
        if content:
           urllistdict = content
           for dic in urllistdict:
               url1 = dic[1]
               if url1!=None and url1!="":
                   uid = dic[0]
                   meta = {"uid":uid}
                   #如果是别的网站URL，这里默认也是请求的，需要加过滤
                   callback = url_funcs.get('.'.join(tldextract.extract(url1)[1:]))
                   if callback:
                       yield scrapy.http.Request(url=url1,callback=callback,meta = meta,dont_filter=True)
                   else:
                       print("start_requests unproduce webset",time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
        else:
            print("no url need to update")
            # time.sleep(3)

    def getDBdata(self):
        check_sql = "select uid, url1 from tb_iprobe_data2 where sflag =0 limit 1"
        self.cur.execute(check_sql)
        check_data = self.cur.fetchall() #([1,2], [4,5], ...)
        print("get data")
        return check_data

    def parse_iqiyi(self,response):
         premiere = ''
         #times ='2018-03-05 15:52:12:998'
         #判断返回状态
         firstresponse = scrapy.Selector(response)
         type1list = firstresponse.xpath('//div[@class="topDot"]/a/h2/text()').extract()
         #视频类型
         type_temp = type1list[0] if type1list else''
         type1 = self.type_dict.get(type_temp,99)
         #影片名称
         MovieTitlelist = firstresponse.xpath('//a[@id="widget-videotitle"]/text()').extract() \
         or firstresponse.xpath('//span[@id="widget-videotitle"]/text()').extract()
         title = MovieTitlelist[0] if MovieTitlelist else ''
         #print title
         #影片导演
         MovieDirectorlist = firstresponse.xpath('//a[@itemprop="director"]/text()').extract()
         director = '/'.join(MovieDirectorlist) if MovieDirectorlist else ''
         self.keylist.extend(MovieDirectorlist)
         #影片主演
         MovieActor = firstresponse.xpath('//p[@class="progInfo_rtp"]/span/a[@itemprop="actor"]/text()').extract()
         self.keylist.extend(MovieActor)
         #插入关键字
         self.insertkey(self.keylist)
         #视频URL
         url = response.url
         #视频资源id
         uid = response.meta["uid"]
         self.itempreduce(uid,url,title,type1,director,premiere)
         print("parse_iqiyi()", url,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))

    #解析tencent
    def parse_tencent(self, response):
        firstresponse = scrapy.Selector(response)
        uid = response.meta["uid"]
        #判断返回状态
        if response.status == 200:
            titleurl =firstresponse.xpath('//h2[@class="player_title"]/a/@href').extract()
            #导演和演员列表
            DirectorActor = firstresponse.xpath('//div[@class="director"]/a/text()').extract()
            self.keylist.extend(DirectorActor)
            self.insertkey(self.keylist)
            #标志
            lag = firstresponse.xpath('//div[@class="director"]/text()').extract()
            director = ''
            if lag:
                count = 0
                for i in lag:
                    if not i.endswith(u'演员: ') :
                        if i == u'/':
                            count =+1
                    else:
                        break
                #导演
                directorlist = DirectorActor[:count+1] if DirectorActor else []
                director = '/'.join(directorlist)
            meta = {"director":director,"uid":uid,"url":response.url}
            fullurl = 'https://v.qq.com'+titleurl[0] if titleurl else ''
            if fullurl != '':
                yield scrapy.http.Request(url=fullurl,callback=self.parse_tencent_second,meta = meta,dont_filter=True)
            else:
                self.itempreduce(uid,'','',99,'','')
                print(u"tencent-else-video",time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
                #小视频等没有详情页面的视频资源

    def parse_tencent_second(self,response): #腾讯二级请求页面
        secondresponse = scrapy.Selector(response)
        #uid
        uid = response.meta["uid"]
        url = response.meta["url"]
        type1list = secondresponse.xpath('//h1[@class="video_title_cn"]/span[@class="type"]/text()').extract()
        type_temp = type1list[0] if type1list else''
        type1 = self.type_dict.get(type_temp,99)
        #影片名称
        MovieTitlelist = secondresponse.xpath('//h1[@class="video_title_cn"]/a/text()').extract()
        title = MovieTitlelist[0] if MovieTitlelist else ''
        #上映时间
        premierelist = secondresponse.xpath('//span[text()="%s"]/following-sibling::span/text()'%(u'出品时间:')).extract() or\
        secondresponse.xpath('//span[text()="%s"]/following-sibling::span/text()'%(u'上映时间:')).extract()
        premiere = premierelist[0]if premierelist else ''
        director = response.meta.get("director")
        self.itempreduce(uid,url,title,type1,director,premiere)
        print("parse_tencent_second()", url,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))

    def parse_youku(self,response):
        print("==================>>>>>>>>>>>>>")
        uid = response.meta["uid"]
        url = response.url
        firstresponse = scrapy.Selector(response)
        if response.status == 200:
            titleurl =firstresponse.xpath('//div[@class="tvinfo"]/h2/a/@href').extract()
            #视频标题
            titlelist = firstresponse.xpath('//div[@class="tvinfo"]/h2/a/text()').extract()
            title = titlelist[0] if titlelist else ''
            #导演和演员列表
            meta = {"uid":uid,"title":title,"url":url}
            fullurl = 'https:'+titleurl[0] if titleurl else ''
            if fullurl != '':
                yield scrapy.http.Request(url=fullurl,callback=self.parse_youku_second,meta = meta,dont_filter=True)
            else:
                self.itempreduce(response.meta["uid"],'','',99,'','')
                print(u"youku-else-video",url,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
                #小视频等没有详情页面的视频资源
    def parse_youku_second(self,response):
        secondresponse = scrapy.Selector(response)
        #uid
        uid = response.meta["uid"]
        #当前页面的URL
        url = response.meta["url"]
        type1list = secondresponse.xpath('//li[@class="p-row p-title"]/a[1]/text()').extract()
        #print "type1list",type1list,type1list[0],type1list[1]
        type_temp = ''.join(type1list) if type1list else''
        #视频类型
        type1 = self.type_dict.get(type_temp,99)
        #print type1
        if type1 ==99:
            self.itempreduce(uid,'','',type1,'','')
            print(u"youku-else-video",url,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
        else:
            #影片名称
            title = response.meta["title"]
            #导演
            directorlist = secondresponse.xpath('//li[text()="%s"]/a/text()'%(u'导演：')).extract()
            director = '/'.join(directorlist) if directorlist else ''
            self.keylist.extend(directorlist)
            #主演
            actorslist = secondresponse.xpath('//li[@class="p-performer"]/@title').extract()
            actors = actorslist[0].split('/') if actorslist else []
            self.keylist.extend(actors)
            self.insertkey(self.keylist)
            #上映时间
            premierelist = secondresponse.xpath('//span[@class="pub"]/label[text()="%s"]/../text()'%(u'上映：')).extract()#../表示当前标签的父标签
            premiere = premierelist[0]if premierelist else ''
            self.itempreduce(uid,url,title,type1,director,premiere)
            print(u"youku-parse_youku_second()",url,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))


    def parse_sohu(self,response):
        url = response.url
        uid = response.meta["uid"]
        meta = {"url":url,"uid":uid}
        firstresponse = scrapy.Selector(response)
        #获取类型(只有电视剧页面有)
        typelist = firstresponse.xpath('//div[@class="crumbs"]/a[1]/text()').extract()
        #判断是会员电影页面（只有电影页面有）
        typefilm = firstresponse.xpath('//div[@class="movie-info-des-wrap"]/h2/text()').extract()
        type_temp = typelist[0] if typelist else ''  #电视剧，综艺等
        if self.type_dict.get(type_temp):#电视剧，非会员电影
            meta["type1"] = self.type_dict[type_temp]
            #标题
            titlelist = firstresponse.xpath('//div[@class="crumbs"]/a[last()]/text()').extract()
            title =  titlelist[0] if titlelist else ''
            meta['title'] = title
            playidcompile = re.compile('var playlistId="(.*)"')
            palylistid = playidcompile.findall(response.text)
            fullurl = 'https://pl.hd.sohu.com/videolist?playlistid='+palylistid[0]+'&order=1&ssl=0&callback=__get_pianhualist' if palylistid else ''
            if fullurl!='':#转向电视剧详情页面
                yield scrapy.http.Request(url=fullurl,callback=self.parse_sohu_second,meta = meta,dont_filter=True)
            else:
                self.itempreduce(response.meta("uid"),'','',99,'','')
                print(u"souhu-parse_sohu()-no-deatil",url,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))

        elif typefilm:#会员电影页面
            type1 = 1
            #title
            titlelist = firstresponse.xpath('//div[@class="movie-t"]/h3/text()').extract()
            #print 'titlelist=====================>',titlelist
            title = titlelist[0] if titlelist else ''
            #主演
            actor_list = firstresponse.xpath('//p[@class="film-text-ellipsis"]/text()').extract()
            #print "sohuactors",actor_list[0]
            temp_a = re.sub("	|\n",'',actor_list[0]) if actor_list else ''
            MovieActor = temp_a.replace(u'主演：','').split('/')
            #print MovieActor
            self.keylist.extend(MovieActor)
            #导演
            content = response.text
            #compile_director = re.compile(u"span>导(.*)</span>")
            compile_director = re.compile(u"导演：\\s+(.*)")
            director_list = compile_director.findall(content)
            self.keylist.extend(director_list)
            self.insertkey(self.keylist)
            director = director_list[0] if director_list else ''
            #年份
            premierelist = firstresponse.xpath('//span[text()="%s"]/em/text()'%(u'年份：')).extract()
            premiere = premierelist[0] if premierelist else ''
            self.itempreduce(uid,url,title,type1,director,premiere)
            print("souhu-vip",url,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))

        else: #其他
            self.itempreduce(uid,'','',99,'','')
            print(u"souhu-else-video",url,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))

    # 电视剧或者非会员电影转向二级页面，js请求
    def parse_sohu_second(self,response):
        secondresponse = response.text
        uid = response.meta["uid"]
        type1 = response.meta["type1"]
        url = response.meta["url"]
        #标题
        title = response.meta["title"]
        #演员
        actorscompile = re.compile(u'\{"actors":\[(.*?)"\]')
        actorlist_temp = actorscompile.findall(secondresponse)
        actorlist=actorlist_temp[0].replace('"','').split(',') if actorlist_temp else []
        self.keylist.extend(actorlist)
        #导演
        directorcompile = re.compile(u'"directors":(.*?\]),"')
        directorlisttemp = directorcompile.findall(secondresponse)
        directorlist = json.loads(directorlisttemp[0]) if directorlisttemp else []
        self.keylist.extend(directorlist)
        self.insertkey(self.keylist)
        director = '/'.join(directorlist)
        #发行年份
        premierecomoile = re.compile(u'\"publishYear":(.*?),"')
        premiere = premierecomoile.findall(secondresponse)[0] if premierecomoile.findall(secondresponse)  else ''
        #发送数据
        self.itempreduce(uid,url,title,type1,director,premiere)
        print(u"souhu-dianshiju or novip-film",url,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))






    def parse_letv(self,response):
        url = response.url
        uid = response.meta["uid"]
        firstresponse = scrapy.Selector(response)
        typelist = firstresponse.xpath('//meta[@name="irCategory"]/@content').extract()
        #类型
        type1 = self.type_dict[typelist[0]] if typelist else 99
        #标题
        titlelist =firstresponse.xpath('//meta[@name="irAlbumName"]/@content').extract()
        title = titlelist[0] if titlelist else ''
        #上映时间
        premierelist = firstresponse.xpath('//b[text()="%s"]/following-sibling::span/a/text()'%(u'上映时间：')).extract()
        premiere = re.sub(' |\n','',premierelist[0]) if premierelist else ''
        #导演 （只有一个的情况）
        directorlist = firstresponse.xpath('//b[text()="%s"]/following-sibling::span/a/text()'%(u'导演：')).extract()
        director = directorlist[0] if directorlist else ''
        self.keylist.extend(directorlist)
        #主演
        actorslist = firstresponse.xpath('//b[text()="%s"]/following-sibling::span/a/text()'%(u'主演：')).extract()
        self.keylist.extend(actorslist)
        self.insertkey(self.keylist)
        self.itempreduce(uid,url,title,type1,director,premiere)
        print("letv",url,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))

    def parse_wasu(self,response):
        meta = {}
        url = response.url
        uid = response.meta["uid"]
        meta["url"] = url
        meta["uid"] = uid
        firstresponse = scrapy.Selector(response)
        typelist = firstresponse.xpath('//div[@class="play_seat"]/a[2]/text()').extract()
        type_temp = typelist[0] if typelist else ''
        type1 = self.type_dict.get(type_temp,99)
        meta["type1"] = type1
        if type1 !=99:#电影或者电视剧，没有上映日期
            #视频标题
            #导演
            directorlisttemp = firstresponse.xpath('//span[text()="%s"]/following-sibling::a/text()'%(u'导演：')).extract()
            directorlist = directorlisttemp[0].split(' ') if directorlisttemp else []
            self.keylist.extend(directorlist)
            director = '/'.join(directorlist)
            meta["director"] = director
            #主演
            actorslisttemp = firstresponse.xpath('//span[text()="%s"]/../@title'%(u'主演：')).extract()#../表示当前标签的父标签
            actorslist = actorslisttemp[0].split(' ') if actorslisttemp else []
            self.keylist.extend(actorslist)
            #关键字插入数据库
            self.insertkey(self.keylist)
            #电影没有上映时间
            if type1 == 1:
                titlelist = firstresponse.xpath('//div[@class="l"]//h3/text()').extract()
                title = titlelist[0] if titlelist else ''
                meta["title"] = title
                premiere = ''
                self.itempreduce(uid,url,title,type1,director,premiere)
                print("wasutv-film",url,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
            else: # 电视剧
                titlelist = firstresponse.xpath('//div[@class="l"]//h3/a/text()').extract()
                title = titlelist[0] if titlelist else ''
                meta["title"] = title
                detailurllist = firstresponse.xpath('//div[@class="one"]/a/@href').extract()
                detailurl = detailurllist[0] if detailurllist else''
                yield scrapy.http.Request(url=detailurl,callback=self.parse_wasu_second,meta = meta,dont_filter=True)
        else:
            self.itempreduce(uid,url,'',type1,'','')
            print(u"wasutv-else-video")
    def parse_wasu_second(self,response):#华数电视剧详细页面获取上映时间
        secondresponse = scrapy.Selector(response)
        #年份
        premierelist = secondresponse.xpath('//p[contains(text(),"%s")]/a/text()'%(u"年份：")).extract()
        premiere = premierelist[0] if premierelist else ''
        #发送数
        self.itempreduce(response.meta["uid"],response.meta["url"],response.meta["title"],response.meta["type1"],response.meta["director"],premiere)
        print(u"wasutv-dianshiju")
    def parse_cntv(self,response):
        meta = {}
        url = response.url
        uid = response.meta["uid"]
        meta["url"] = url
        meta["uid"] = uid
        firstresponse = scrapy.Selector(response)
        if url.startswith( 'http://tv.cntv.cn/video' ):#这种是部分电视剧和电影的地址
            titlelist = firstresponse.xpath('//div[@class="bread"]/a[2]/text()').extract()
            title = titlelist[0] if titlelist else ''
            meta["title"] = title
            #去搜索框搜索
            search_url = 'http://search.cctv.com/search.php?qtext='+title+'&sid=0021&pid=0000'
            yield scrapy.http.Request(url=search_url,callback=self.parse_search_cntv,meta = meta,dont_filter=True)
        elif url.startswith( 'http://tv.cctv.com' ):#电视剧地址
            titlelist = firstresponse.xpath('//a[@id="videoalbumId"]/text()').extract()
            title = titlelist[0].strip() if titlelist else ''
            meta["title"] = title
            detailurllist = firstresponse.xpath('//a[@id="videoalbumId"]/@href').extract()
            detailurl = detailurllist[0] if detailurllist else ''
            #进入电视剧详情页
            if detailurl !='':
                yield scrapy.http.Request(url=detailurl,callback=self.parse_comdetail_cntv,meta = meta,dont_filter=True)
            else:
                self.itempreduce(uid,url,title,99,'','')#0代表未知类型
                print("cntv no detail and patch data")
                #发送空数据
        else:#其他地址，暂不处理
            self.itempreduce(uid,url,'',99,'','')
            print("cntv-else-unproduce")

    def parse_search_cntv(self,response):
        searchresponse = scrapy.Selector(response)
        meta = response.meta
        #校验一下搜索页的标题是否和播放页面一致
        search_titlelist = searchresponse.xpath('//h3[@class="tit"]/a/font/text()').extract()
        search_title = search_titlelist[0] if search_titlelist else 'null'
        if search_title == response.meta['title']:
            detailurllist = searchresponse.xpath('//p[@class="more"]/a/@href').extract()
            #urlcompile = re.compile('link.php?targetpage=(.*)&point')
            urlcompile = re.compile('link.php\?targetpage=(.*)&point')
            urllisttemp = urlcompile.findall(detailurllist[0])if detailurllist else []
            detailurl = urllisttemp[0] if urllisttemp else''
            #判断URL类型，有三种，/dianying.cntv.cn转向电影页面,tv.cntv.com转向另一种板式，tv.cntv.cn没有详情
            if detailurl!=''and detailurl.startswith('http://tv.cctv.com'):
                print("turn to the dianshiju")
                yield scrapy.http.Request(url=detailurl,callback=self.parse_comdetail_cntv,meta = meta,dont_filter=True)
            elif detailurl!=''and detailurl.startswith('http://dianying.cntv.cn'):#转向电影页面
                print(u"turn to the film")
                yield scrapy.http.Request(url=detailurl,callback=self.parse_dianyingdetail_cntv,meta = meta,dont_filter=True)
            elif detailurl!=''and detailurl.startswith('http://tv.cntv.cn/videoset'):
                print("detail is tv.cntv.cn,no detail")# 这里转向的地址没有详情，只发送标题，其余数据为空
                self.itempreduce(meta["uid"],meta["url"],search_title,0,'','')#0代表未知类型
            else:
                print("cntv detail url is unknown") #没有详情URL
                self.itempreduce(meta["uid"],meta["url"],search_title,0,'','')#0代表未知类型
        else:#搜索为空
            self.itempreduce(meta["uid"],meta["url"],response.meta['title'],0,'','')
            print u"cntv search null"
    def parse_comdetail_cntv(self,response):
        detailresponse = scrapy.Selector(response)
        #导演
        directorlisttemp = detailresponse.xpath('//span[text()="%s"]/../text()'%(u'导演：')).extract()
        directorlist = directorlisttemp[0].split(',') if directorlisttemp else []
        self.keylist.extend(directorlist)
        director = director = '/'.join(directorlist)
        #主演
        actorlisttemp = detailresponse.xpath('//span[text()="%s"]/../text()'%(u'主演：')).extract()
        actorlist = actorlisttemp[0].split(',') if directorlisttemp else []
        self.keylist.extend(actorlist)
        #插入数据库
        self.insertkey(self.keylist)
        #年份
        premierelist = detailresponse.xpath('//span[text()="%s"]/../text()'%(u'年份：')).extract()
        premiere = premierelist[0] if premierelist else ''
        title =  response.meta["title"]
        uid = response.meta["uid"]
        url = response.meta["url"]
        #集数
        type1 = 2 if detailresponse.xpath('//span[text()="%s"]/../text()'%(u'集数：')).extract() else 1
        #发送数据
        self.itempreduce(uid,url,title,type1,director,premiere)
        print(u"==========>cntv film or dianshiju",url,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))

    def parse_dianyingdetail_cntv(self,response):
        dianyingresponse = scrapy.Selector(response)
        lagcompile = re.compile(u'<td class="js"><a href!="">(.*?)</a><')
        lag = lagcompile.findall(response.text)
        #导演
        director = lag[3] if lag else ''
        self.keylist.append(director)
        actorslist = lag[4].split(',') if lag[4] else []
        self.keylist.extend(actorslist)
        #插入数据库
        self.insertkey(self.keylist)
        #年份
        premiere = lag[5] if lag[5] else ''
        #类别
        type1 = 1
        #title
        title = response.meta['title']
        uid = response.meta['uid']
        url = response.meta['url']
        self.itempreduce(uid,url,title,type1,director,premiere)
        print "cntv-film",url,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))




    def unprocess_url(self,response):
        print("unknown")



    #将演员和关键字插入库中
    def insertkey(self,insertkey):
        if insertkey:
            for key in list(set(insertkey)):
                check_sql = "select keyword from public.tb_movie_keyword_task where keyword ='%s'"%key
                self.cur.execute(check_sql)
                check_data = self.cur.fetchall()
                if check_data:  # 关键字存在的话就不插入
                    print('keyword exists')
                else:
                    self.cur.execute(u"insert into public.tb_movie_keyword_task(keyword, flag) values('{}', {})".format(key,0))
                    print('keyword insert ok')
                self.db.commit()
        else:
            return

    #将需要的内容更新回数据库
    def itempreduce(self, uid, url, title, type1, director, premiere):
        times = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))

        #更新到本地postgres
        check_sql = "update tb_iprobe_data2 set url = '%s', title= '%s', director= '%s', premiere = '%s', type1 = '%d', sflag = 2" \
                    " where uid = '%s' and sflag = 0"%(url, title, director, premiere, type1, uid)
        self.cur.execute(check_sql)
        self.db.commit()
        print("更新数据库tb_iprobe_data2[%s]"%times)