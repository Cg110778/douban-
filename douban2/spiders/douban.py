# -*- coding: utf-8 -*-
import pymongo
import re

import scrapy
from scrapy import Request,Spider
from douban2.items import *

class DoubanSpider(Spider):
    name = 'douban2'
    allowed_domains = ['douban.com']
    start_urls = ['http://douban.com/people/{uid}/']
    contacts_url= 'https://www.douban.com/people/{uid}/contacts'
    rev_contacts_url = 'https://www.douban.com/people/{uid}/rev_contacts'
    user_url = 'http://douban.com/people/{uid}/'
    start_users = ['128290489']#'57109633',#'1693422','50446886'ninetonine''137546285'183501886'#'3452208'新桥，宋史128290489
    movie_do_url = 'https://movie.douban.com/people/{uid}/do'
    movie_wish_url ='https://movie.douban.com/people/{uid}/wish'
    movie_collect_url ='https://movie.douban.com/people/{uid}/collect'
    music_do_url ='https://music.douban.com/people/{uid}/do'
    music_wish_url ='https://music.douban.com/people/{uid}/wish'
    music_collect_url ='https://music.douban.com/people/{uid}/collect'
    book_do_url ='https://book.douban.com/people/{uid}/do'
    book_wish_url ='https://book.douban.com/people/{uid}/wish'
    book_collect_url ='https://book.douban.com/people/{uid}/collect'
    contacts_id=[]

    def get_id(self,collection, database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        #.skip(100)
        users_id= collection.find({},{'_id':0,'id':1}).limit(1).skip(1)
        return users_id
        # print(users_id)
    def get_movie_urls(self,collection,database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        movie_url=collection.find({},{'_id':0,'movie_url':1})
        return movie_url

    def get_music_urls(self,collection,database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        music_url=collection.find({},{'_id':0,'music_url':1})
        return music_url

    def get_book_urls(self,collection,database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        book_url=collection.find({},{'_id':0,'book_url':1})
        return book_url


    def start_requests(self):
        for uid in self.start_users:
            yield Request(self.user_url.format(uid=uid), callback=self.parse_user)


    def parse_user(self,response):
        uids=self.get_id(collection='users',database='new_douban')
        for uid in uids:
            if uid:
                uid=uid['id']
                print(uid)
                #uid='128290489'
                yield Request(self.movie_do_url.format(uid=uid),callback=self.parse_movie_link)
                #yield Request(self.movie_wish_url.format(uid=uid), callback=self.parse_movie_link)
                yield Request(self.movie_collect_url.format(uid=uid), callback=self.parse_movie_link)
                yield Request(self.music_do_url.format(uid=uid), callback=self.parse_music_link)
                #yield Request(self.music_wish_url.format(uid=uid), callback=self.parse_music_link)
                yield Request(self.music_collect_url.format(uid=uid), callback=self.parse_music_link)
                yield Request(self.book_do_url.format(uid=uid), callback=self.parse_book_link)
                #yield Request(self.book_wish_url.format(uid=uid), callback=self.parse_book_link)
                yield Request(self.book_collect_url.format(uid=uid), callback=self.parse_book_link)



    def parse_movie_link(self,response):
        id = response.xpath('//*[@id="db-usr-profile"]/div[@class="info"]//li[1]/a/@href').extract_first()
        id = re.search('.*?/people/(.*?)/', id)
        id = id.group(1)
        movie_link=response.xpath(
            '//*[@id="content"]//div[@class="article"]//a[@class="nbg"]/@href').extract()
        #电影链接
        for i in movie_link:
            next_page = response.xpath(
                '//*[@id="content"]//div[@class="paginator"]/span[@class="next"]//a[contains(.,"后页")]/@href').extract_first()
            if next_page:
               if 'douban.com' in next_page:
                   yield Request(url=next_page, callback=self.parse_movie_link)
               else:
                    next_page_url = 'https://movie.douban.com' + next_page
                    # print(next_page,response.url)
                    yield Request(url=next_page_url, callback=self.parse_movie_link)
                    # 下一页subject列表'''
            yield Request(url=i,callback=self.parse_movie)#电影简介'''



    def parse_music_link(self, response):
        id = response.xpath('//*[@id="db-usr-profile"]/div[@class="info"]//li[1]/a/@href').extract_first()
        id = re.search('.*?/people/(.*?)/', id)
        id = id.group(1)
        music_link = response.xpath('//*[@id="content"]//div[@class="article"]//a[@class="nbg"]/@href').extract()
        # 音乐链接
        for i in music_link:
            next_page = response.xpath(
                '//*[@id="content"]//div[@class="paginator"]/span[@class="next"]//a[contains(.,"后页")]/@href').extract_first()
            if next_page:
                if 'douban.com' in next_page:
                    yield Request(url=next_page, callback=self.parse_music_link)
                else:
                    next_page_url = 'https://music.douban.com' + next_page
                    yield Request(url=next_page_url, callback=self.parse_music_link)
                    # 下一页subject列表'''
            yield Request(url=i, callback=self.parse_music)#音乐简介



    def parse_book_link(self, response):
        id = response.xpath('//*[@id="db-usr-profile"]/div[@class="info"]//li[1]/a/@href').extract_first()
        id = re.search('.*?/people/(.*?)/', id)
        id = id.group(1)
        book_link = response.xpath('//*[@id="content"]//div[@class="article"]//a[@class="nbg"]/@href').extract()
        # 书籍链接
        for i in book_link:
            next_page = response.xpath(
                '//*[@id="content"]//div[@class="paginator"]/span[@class="next"]//a[contains(.,"后页")]/@href').extract_first()
            if next_page:
                if 'douban.com' in next_page:
                    yield Request(url=next_page, callback=self.parse_book_link)
                else:
                    next_page_url = 'https://book.douban.com' + next_page
                    yield Request(url=next_page_url, callback=self.parse_book_link)
                    # 下一页subject列表'''
            yield Request(url=i, callback=self.parse_book)#书籍简介


    def parse_movie(self,response):
        item = DoubandetailmovieItem()
        item['movie_url'] = response.url
        movie_id = re.search('https://movie.douban.com/subject/(.*?)/', response.url)
        movie_id = movie_id.group(1)
        item['movie_id'] = movie_id
        item['movie_name'] = response.xpath('//span[@property="v:itemreviewed"]/text()').extract_first()
        item['movie_playbill'] = response.xpath('//*[@id="mainpic"]/a/img/@src').extract_first()
        item['movie_director'] = response.xpath('//a[@rel="v:directedBy"]/text()').extract()
        item['movie_scriptwriter'] = response.xpath('//*[@id="info"]/span[2]/span[@class="attrs"]/a/text()').extract()
        item['movie_starring'] = response.xpath('//a[@rel="v:starring"]/text()').extract()
        item['movie_type'] = response.xpath('//span[@property="v:genre"]/text()').extract()
        item['movie_producer_countryORregion'] = response.selector.re(re.compile('<span.*?>制片国家/地区:</span>(.*?)<br>'))
        item['movie_language'] = response.selector.re(re.compile('<span.*?>语言:</span>(.*?)<br>'))
        item['movie_date'] = response.xpath('//span[@property="v:initialReleaseDate"]/text()').extract_first()
        item['movie_season'] = response.selector.re(re.compile('<span.*?>季数:</span>(.*?)<br>'))
        item['movie_episodes'] = response.selector.re(re.compile('<span.*?>集数:</span>(.*?)<br>'))
        item['movie_single_episode_length'] = response.selector.re(re.compile('<span.*?>单集片长:</span>(.*?)<br>'))
        item['movie_length'] = response.xpath('//span[@property="v:runtime"]/text()').extract_first()
        item['movie_alias'] = response.selector.re(re.compile('<span.*?>又名:</span>(.*?)<br>'))
        item['movie_IMDb'] = response.xpath('//*[@id="info"]/a/@href').extract_first()
        item['movie_star'] = response.xpath('//strong[@property="v:average"]/text()').extract_first()
        item['movie_5score'] = response.xpath(
            '//span[@class="stars5 starstop"]/../span[@class="rating_per"]/text()').extract_first()
        item['movie_4score'] = response.xpath(
            '//span[@class="stars4 starstop"]/../span[@class="rating_per"]/text()').extract_first()
        item['movie_3score'] = response.xpath(
            '//span[@class="stars3 starstop"]/../span[@class="rating_per"]/text()').extract_first()
        item['movie_2score'] = response.xpath(
            '//span[@class="stars2 starstop"]/../span[@class="rating_per"]/text()').extract_first()
        item['movie_1score'] = response.xpath(
            '//span[@class="stars1 starstop"]/../span[@class="rating_per"]/text()').extract_first()
        item['movie_describe'] = ''.join(response.xpath('//*[@id="link-report"]/span/text()').extract()).replace('\n',
                                                                                                                 '').strip()
        item['movie_comment_number'] = response.xpath(
            '//*[@id="comments-section"]//span[@class="pl"]/a/text()').extract_first()
        yield item

    def parse_music(self,response):
        item = DoubandetailmusicItem()
        item['music_url'] = response.url
        music_id = re.search('https://music.douban.com/subject/(.*?)/', response.url)
        music_id = music_id.group(1)
        item['music_id'] = music_id
        item['music_alias'] = ''.join(response.selector.re(re.compile('<span.*?>又名:</span>(.*?).<br>', re.S))).strip()
        item['music_name'] = response.xpath('//*[@id="wrapper"]/h1/span/text()').extract_first()
        item['music__playbill'] = response.xpath('//*[@id="mainpic"]/span/a/img/@src').extract_first()
        item['music_performer'] = response.xpath(
            '//*[@id="content"]//*[@id="info"]//span[@class="pl"]/a/text()').extract()
        item['music_type'] = ''.join(response.selector.re(re.compile('<span.*?>流派:</span>(.*?)<br>', re.S))).strip()
        item['music_album_type'] = ''.join(
            response.selector.re(re.compile('<span.*?>专辑类型:</span>(.*?)<br>', re.S))).strip()
        item['music_medium'] = ''.join(response.selector.re(re.compile('<span.*?>介质:</span>(.*?)<br>', re.S))).strip()
        item['music_date'] = ''.join(response.selector.re(re.compile('<span.*?>发行时间:</span>(.*?)<br>', re.S))).strip()
        item['music_publisher'] = ''.join(
            response.selector.re(re.compile('<span.*?>出版者:</span>(.*?)<br>', re.S))).strip()
        item['music_number_of_records'] = ''.join(
            response.selector.re(re.compile('<span.*?>唱片数:</span>(.*?)<br>', re.S))).strip()
        item['music_barcode'] = ''.join(response.selector.re(re.compile('<span.*?>条形码:</span>(.*?)<br>', re.S))).strip()
        item['music_other_versions'] = ''.join(
            response.selector.re(re.compile('<span.*?>其他版本:</span>(.*?)<br>', re.S))).strip()
        item['music_star'] = response.xpath('//strong[@property="v:average"]/text()').extract_first()
        item['music_5score'] = response.xpath('//span[@class="rating_per"][1]/text()').extract_first()
        item['music_4score'] = response.xpath('//span[@class="rating_per"][2]/text()').extract_first()
        item['music_3score'] = response.xpath('//span[@class="rating_per"][3]/text()').extract_first()
        item['music_2score'] = response.xpath('//span[@class="rating_per"][4]/text()').extract_first()
        item['music_1score'] = response.xpath('//span[@class="rating_per"][5]/text()').extract_first()
        item['music_describe'] = ''.join(response.xpath('//*[@id="link-report"]/span/text()').extract()).replace('\n', '').strip()
        item['music_comment_number'] = response.xpath(
            '//*[@id="content"]//div[@class="mod-hd"]//span[@class="pl"]/a/text()').extract_first()
        yield item

    def parse_book(self,response):
        item = DoubandetailbookItem()
        item['book_url'] = response.url
        book_id = re.search('https://book.douban.com/subject/(.*?)/', response.url)
        book_id = book_id.group(1)
        item['book_id'] = book_id
        item['book_url'] = response.url
        item['book_name'] = response.xpath('//*[@id="wrapper"]/h1/span/text()').extract_first()
        item['book_playbill'] = response.xpath('//*[@id="mainpic"]/a/img/@src').extract_first()
        item['book_star'] = response.xpath('//strong[@property="v:average"]/text()').extract_first()
        item['book_5score'] = response.xpath('//span[@class="rating_per"][1]/text()').extract_first()
        item['book_4score'] = response.xpath('//span[@class="rating_per"][2]/text()').extract_first()
        item['book_3score'] = response.xpath('//span[@class="rating_per"][3]/text()').extract_first()
        item['book_2score'] = response.xpath('//span[@class="rating_per"][4]/text()').extract_first()
        item['book_1score'] = response.xpath('//span[@class="rating_per"][5]/text()').extract_first()
        item['book_describe'] = ''.join(
            response.xpath('//div[@id="link-report"]//div[@class="intro"]//text()').extract()).replace(
            '\n', '').strip()
        item['book_comment_number'] = response.xpath(
            '//*[@id="content"]//div[@class="mod-hd"]//span[@class="pl"]/a/text()').extract_first()
        datas = response.xpath("//div[@id='info']//text()").extract()
        datas = [data.strip() for data in datas]
        datas = [data for data in datas if data != ""]
        # 打印每一项内容
        # for i, data in enumerate(datas):
        # print "index %d " %i, data
        for data in datas:
            if u"作者" in data:
                if u":" in data:
                    item['book_author'] = ''.join(datas[datas.index(data) + 1]).strip()
                elif u":" not in data:
                    item['book_author'] = ''.join(datas[datas.index(data) + 2]).strip()
            elif u"出版社:" in data:
                item['book_publisher'] = datas[datas.index(data) + 1]
            elif u"译者:" in data:
                if u":" in data:
                    item['book_translator'] = datas[datas.index(data) + 1]
                elif u":" not in data:
                    item['book_translator'] = datas[datas.index(data) + 2]
            elif u"出版年:" in data:
                item['book_date'] = datas[datas.index(data) + 1]
            elif u"页数:" in data:
                item['book_page_number'] = datas[datas.index(data) + 1]
            elif u"定价:" in data:
                item['book_pricing'] = datas[datas.index(data) + 1]
            elif u"装帧:" in data:
                item['book_binding'] = datas[datas.index(data) + 1]
            elif u"丛书:" in data:
                if u":" in data:
                    item['book_series'] = datas[datas.index(data) + 1]
                elif u":" not in data:
                    item['book_series'] = datas[datas.index(data) + 2]
            elif u"ISBN:" in data:
                item['book_ISBN'] = datas[datas.index(data) + 1]
        yield item



