import json
import time
import itertools
from itertools import product

import psycopg2
from lxml import html

import scrapy
from scrapy import signals
from scrapy.exceptions import DontCloseSpider

from scrapy_redis.spiders import RedisSpider

from demo3_product.items import Demo3ProductItem
from demo3_product.settings import POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DBNAME


def read_line():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DBNAME,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
                UPDATE spider_temp
                SET status = 10
                WHERE id IN (
                    SELECT id FROM spider_temp
                    WHERE status = 1 AND domain='lucyinthesky.com'
                    LIMIT 100
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, link, tags, referer;
            """)
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return rows


class LucyintheskySpider(RedisSpider):
    name = "lucyinthesky"
    task_id = 5
    offset = 0
    is_finished = False
    redis_key = "lucyinthesky:start_urls"

    allowed_domains = ["www.lucyinthesky.com", "media-img.lucyinthesky.com", "api.lucyinthesky.com"]

    def make_request_from_data(self, data):
        """
        scrapy-redis 默认只支持 URL 字符串
        我们重写这个方法，让它能解析 JSON
        """
        try:
            task = json.loads(data)
        except json.JSONDecodeError:
            # 如果不是 JSON，就当作普通 URL 处理
            return scrapy.Request(url=data.decode(), callback=self.parse)

        url = task.get("url")
        headers = task.get("headers", {})
        meta = task.get("meta", {})

        return scrapy.Request(
            url=url,
            headers=headers,
            meta=meta,
            callback=self.parse
        )

    def parse(self, response, **kwargs):
        meta = response.meta
        item = Demo3ProductItem()
        nnow = int(time.time())

        script = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        if not script:
            self.logger.warning("找不到 __NEXT_DATA__ 脚本")
            return

        try:
            script_data = json.loads(script)
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON 解析失败: {e}")
            return

        product_data = script_data['props']['pageProps']['store']['cueoq7nz']

        title = product_data['name']
        description = product_data['description'].replace('\u003c', '<').replace('\u003e', '>')
        price = product_data['priceValue']
        images = [
            {
                "id": i["id"],
                "src": "https://media-img.lucyinthesky.com" + i["image"],
                "position": _i + 1,
            } for _i, i in enumerate(product_data['images'])
        ]

        pid = product_data["id"]


        colors_set = set()
        product_color = script_data['props']['pageProps']['store']['4cro23qr']
        for color_item in product_color:
            for col in color_item['colors']:
                cn = col['name']
                colors_set.add(cn)

        colors = list(colors_set)


        item["task_id"] = 5
        item["user_id"] = 3
        item["cid"] = 1
        item["domain"] = 'lucyinthesky.com'
        item["title"] = title
        item["handle"] = response.url.split('/')[-1]
        item["description"] = description
        item["vendor"] = 'lucyinthesky'
        item["category"] = json.dumps(meta.get("tags"))
        item["original_price"] = float(price)
        item["current_price"] = float(price)
        item["images"] = json.dumps(images)
        # item["variants"] = json.dumps(variants)
        item["tags"] = '[]'
        item["created_at"] = nnow
        item["updated_at"] = nnow
        item["type"] = 1
        item["platform"] = 4
        # item["options"] = json.dumps(options)
        item["postid"] = meta.get("postid")
        self.logger.info(f"✅ get product item: {title}")
        yield scrapy.Request(
            url=f"https://api.lucyinthesky.com/catalog/product/stock/{pid}?countryId=223",
            callback=self.parsel_size,
            meta={'item': item, "colors": colors},
        )

    def parsel_size(self, response, **kwargs):
        meta = response.meta
        item = meta["item"]
        colors = meta["colors"]

        sizes = [si['size'] for si in response.json()]

        options = []
        if colors and sizes:
            variants = [
                {
                    "title": f"{c} / {s}",
                    "price": str(item["original_price"]),
                    "weght": "",
                    "barcode": "",
                    "curreny": "",
                    "option1": c,
                    "option2": s,
                    "option3": None,
                    "image_id": "",
                    "position": __i + 1,
                    "weght_unit": "",
                    "compare_at_price": None
                }
                for __i, (c, s) in enumerate(itertools.product(colors, sizes))
            ]
            options.append({
                "name": "color",
                "values": colors,
                "position": 1
            })
            options.append({
                "name": "size",
                "values": sizes,
                "position": 2
            })
        elif colors:
            variants = [
                {
                    "title": f"{c}",
                    "price": str(item["original_price"]),
                    "weght": "",
                    "barcode": "",
                    "curreny": "",
                    "option1": c,
                    "option2": None,
                    "option3": None,
                    "image_id": "",
                    "position": __i + 1,
                    "weght_unit": "",
                    "compare_at_price": None
                }
                for __i, c in enumerate(colors)
            ]
            options.append({
                "name": "color",
                "values": colors,
                "position": 1
            })
        elif sizes:
            variants = [
                {
                    "title": f"{s}",
                    "price": str(item["original_price"]),
                    "weght": "",
                    "barcode": "",
                    "curreny": "",
                    "option1": s,
                    "option2": None,
                    "option3": None,
                    "image_id": "",
                    "position": __i + 1,
                    "weght_unit": "",
                    "compare_at_price": None
                }
                for __i, s in enumerate(sizes)
            ]
            options.append({
                "name": "size",
                "values": sizes,
                "position": 1
            })
        else:
            variants = []

        item["variants"] = json.dumps(variants)
        item["options"] = json.dumps(options)
        yield item



