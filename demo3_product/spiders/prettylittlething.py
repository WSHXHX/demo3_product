import json
import time
from typing import Any

import scrapy
from scrapy.http import Response
from scrapy_redis.spiders import RedisSpider

from demo3_product.items import Demo3ProductItem


class HelloMollySpider(RedisSpider):
    name = "prettylittlething"
    domain = "prettylittlething.us"
    task_id = 10
    redis_key = f"{name}:start_urls"
    allowed_domains = ["prettylittlething.us", ]

    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
    }

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
                return scrapy.Request(url=data.decode("latin-1"), callback=self.parse, headers=self.headers)

        try:
            task = json.loads(data)
        except json.JSONDecodeError:
            # 如果不是 JSON，就当成 URL 字符串
            return scrapy.Request(url=data, callback=self.parse, headers=self.headers)

        url = task.get("url")
        headers = task.get("headers", {})
        headers.update(self.headers)
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
            self.logger.warning("找不到 script 脚本")
            return

        try:
            script_data = json.loads(script)
        except json.JSONDecodeError as e:
            self.logger.error(f"没找到数据: {response.url} {e}")
            return


        product_data = script_data['props']['pageProps']['data']

        title = product_data['name']
        handle = product_data['urlKey']
        description = product_data['description'].replace('\u003c', '<').replace('\u003e', '>')

        colors = [product_data["colour"]["colourEn"], ]
        sizes = [i["size"] for i in product_data["sizes"]]
        price = float(product_data["pricing"]["price"].replace("$", "").replace("*", ""))


        variants = []
        __i = 0
        for color in colors:
            for size in sorted(sizes):
                variant = {
                    "title": f"{color} / {size}",
                    "price": str(price),
                    "weght": "",
                    "barcode": "",
                    "curreny": "",
                    "option1": color,
                    "option2": size,
                    "option3": None,
                    "image_id": "",
                    "position": __i + 1,
                    "weght_unit": "",
                    "compare_at_price": None
                }
                variants.append(variant)
                __i += 1

        category = meta.get("tags") + [product_data["subcategory"], product_data["category"], product_data["style"]]
        category = list(set(category))

        images = [
            {
                "id": f"img_{_i}",
                "src": img,
                "position": f"{_i}",
            } for _i, img in enumerate(product_data["images"]["thumbnails"])
        ]


        options = [
            {
                "name": "color",
                "values": colors,
                "position": 1
            },
            {
                "name": "size",
                "values": sizes,
                "position": 2
            }
        ]

        item["task_id"] = self.task_id
        item["user_id"] = 3
        item["cid"] = 1
        item["domain"] = self.domain
        item["title"] = title
        item["handle"] = handle
        item["description"] = description
        item["vendor"] = self.name
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
