# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class VideowebsetItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    uid = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()          #影片标题
    type1 = scrapy.Field()          #类型
    director = scrapy.Field()      #导演
    create_time = scrapy.Field()    #时间
    premiere = scrapy.Field()       #编剧