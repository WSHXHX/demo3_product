import json
import time
import itertools
from typing import Any

import scrapy
from scrapy.http import Response
from scrapy_redis.spiders import RedisSpider

from demo3_product.items import Demo3ProductItem


class HelloMollySpider(RedisSpider):
    name = "hellomolly"
    domain = "hellomolly.com"
    task_id = 6
    redis_key = "hellomolly:start_urls"
    allowed_domains = ["www.hellomolly.com", "searchspring.io"]

    def make_request_from_data(self, data):
        """
        scrapy-redis 默认只支持 URL 字符串
        我们重写这个方法，让它能解析 JSON
        """
        if isinstance(data, bytes):
            try:
                data = data.decode("utf-8")
            except UnicodeDecodeError:
                # 不能 UTF-8 解码 → 当作普通 URL 处理
                return scrapy.Request(url=data.decode("latin-1"), callback=self.parse)

        try:
            task = json.loads(data)
        except json.JSONDecodeError:
            # 如果不是 JSON，就当成 URL 字符串
            return scrapy.Request(url=data, callback=self.parse)

        url = task.get("url")
        headers = task.get("headers", {})
        meta = task.get("meta", {})

        return scrapy.Request(
            url=url,
            headers=headers,
            meta=meta,
            callback=self.parse
        )

    def parse(self, response: Response, **kwargs: Any) -> Any:
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

        product_data = script_data['props']['pageProps']['product']

        title = product_data['title']
        handle = product_data['handle']
        description = product_data['descriptionHtml'].replace('\u003c', '<').replace('\u003e', '>')
        price = float(product_data['price']['amount'])
        if not price:
            price = float(product_data['compareAtPrice']['amount'])

        category = product_data['tags']

        images = [
            {
                "id": i["id"],
                "src": i["url"],
                "position": _i + 1,
            } for _i, i in enumerate(product_data['images'])
        ]
        variants = [
            {
                "title": v['sku'].replace('-', ' / '),
                "price": v["price"]["amount"],
                "weght": "",
                "barcode": "",
                "curreny": "",
                "option1": v["size"],
                "option2": None,
                "option3": None,
                "image_id": "",
                "position": _i + 1,
                "weght_unit": "",
                "compare_at_price": None
            } for _i, v in enumerate(product_data['variants'])
        ]

        options = [
            {
                "name": "size",
                "values": [v['size'] for v in product_data['variants']],
                "position": 1
            }
        ]

        item["task_id"] = self.task_id
        item["user_id"] = 5
        item["cid"] = 1
        item["domain"] = 'hellomolly.com'
        item["title"] = title
        item["handle"] = handle
        item["description"] = description
        item["vendor"] = 'hellomolly'
        item["category"] = json.dumps(category)
        item["original_price"] = float(price)
        item["current_price"] = float(price)
        item["images"] = json.dumps(images)
        item["variants"] = json.dumps(variants)
        item["tags"] = '[]'
        item["created_at"] = nnow
        item["updated_at"] = nnow
        item["type"] = 1
        item["platform"] = 4
        item["options"] = json.dumps(options)
        item["postid"] = meta.get("postid")
        self.logger.info(f"✅ get product item: {title}")
        yield item
