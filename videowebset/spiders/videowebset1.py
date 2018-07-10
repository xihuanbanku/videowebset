#!/usr/bin/env python
# coding=utf-8
import scrapy
import requests
import json
import time
import random
import tld
from tld import get_tld

class Videowebset1Spider(scrapy.Spider):
    name = 'videowebset1'


    def start_requests(self):
        url_funcs  = {
			"iqiyi.com":self.parse_iqiyi,
            "qq.com":self.parse_tencent,
            "youku.com":self.parse_youku
		}

        urllist = ["http://www.iqiyi.com","https://v.qq.com/tv/yingmei/",
        "http://v.youku.com/v_show/id_XMzQwMjgyNjQ2MA==.html?spm=a2hww.20027244.m_250379.5~1~3~A"]
        for url in urllist: #遍历100个URL列表
            yield scrapy.http.Request(url=url,callback=url_funcs.get(get_tld(url)) if url_funcs.get(get_tld(url)) else self.unprocess_url)
    
    def parse_iqiyi(self,response):
         #print "iqiyi"
         #time.sleep(19)
         count = 0
         print response.status
         while (count<1000):
             count+=1
         print "iqiyi"
         selector = scrapy.Selector(response)
         print selector
         #meta = {"jiexi_data":jiexi_data}
         #yield Post(self.jiekou_url,callback=self.process_result,meta=meta)
    def parse_tencent(self,response):
         print "tencent"
         selector = scrapy.Selector(response)
         print selector
         #meta = {"jiexi_data":jiexi_data}
         #yield Post(self.jiekou_url,callback=self.process_result,meta=meta)
    def parse_youku(self,response):
         print "youku"
         selector = scrapy.Selector(response)
         print selector         
    def unprocess_url(self,response):
         print "unknown"
