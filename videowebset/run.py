#coding:utf-8
import subprocess
import time

from scrapy import cmdline

while True:
    try:
        cmdline.execute('scrapy crawl videowebset'.split())
        # subprocess.call('scrapy crawl videowebset')
    except Exception as e:
        print e
    time.sleep(5)