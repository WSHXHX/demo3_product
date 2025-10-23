# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Demo3ProductItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    task_id = scrapy.Field()
    user_id = scrapy.Field()
    cid = scrapy.Field()
    domain = scrapy.Field()
    title = scrapy.Field()
    handle = scrapy.Field()
    description = scrapy.Field()
    vendor = scrapy.Field()
    category = scrapy.Field()
    original_price = scrapy.Field()
    current_price = scrapy.Field()
    images = scrapy.Field()
    variants = scrapy.Field()
    tags = scrapy.Field()
    created_at = scrapy.Field()
    updated_at = scrapy.Field()
    type = scrapy.Field()
    platform = scrapy.Field()
    options = scrapy.Field()

    mysqlid = scrapy.Field()
    postid = scrapy.Field()

