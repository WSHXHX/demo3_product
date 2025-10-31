import json
import time
from typing import Any

import scrapy
from scrapy.http import Response
from scrapy_redis.spiders import RedisSpider

from demo3_product.items import Demo3ProductItem


class HouseofcbSpider(RedisSpider):
    name = "houseofcb"
    domain = "houseofcb.com"
    task_id = 7
    redis_key = f"{name}:start_urls"
    allowed_domains = ["houseofcb.com",]

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

        scripts = response.xpath('//script/text()').getall()
        if not scripts:
            self.logger.warning("找不到 script 脚本")
            return

        for script in scripts:
            try:
                script_data = json.loads(script.split(':', 1)[1][:-5])
                break
            except json.JSONDecodeError as e:
                pass
            except IndexError as e:
                pass
        else:
            self.logger.error(f"没找到数据: {response.url}")
            return

        product_data = script_data['state']['loaderData']['routes/product/$slug']['product']

        title = product_data['name']
        handle = product_data['slug']
        description = product_data['description'].replace('\u003c', '<').replace('\u003e', '>')

        colors = []
        for attr in product_data['derivedAttributes']:
            if attr['name'] == 'color':
                colors = attr['values']
                break

        size_price_map = {}

        for variant in product_data['variants']:
            # 提取尺寸
            size = variant['attributes']['size']

            # 提取价格并转换为最终价格
            price_info = variant['prices']
            cent_amount = price_info['centAmount']
            fractional_digits = price_info['fractionalDigits']

            # 转换为最终价格格式
            final_price = cent_amount / (10 ** fractional_digits)
            size_price_map[size] = final_price

        sizes = sorted(size_price_map.keys())
        prices = [size_price_map[size] for size in sizes]

        variants = []
        __i = 0
        for color in colors:
            for size in sorted(size_price_map.keys()):
                variant = {
                    "title": f"{color} / {size}",
                    "price": str(size_price_map[size]),
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

        price = float(min(prices))
        category = meta.get("tags") + [i["name"] for i in product_data["categories"]] + [i["slug"] for i in product_data["categories"]]
        category = list(set(category))

        images = []
        for i, variant in enumerate(product_data['variants']):
            # 只从主变体（master variant）提取图片，避免重复
            if variant.get('isMaster') or i == 0:  # 第一个变体或者标记为master的变体
                for j, image_data in enumerate(variant['images']):
                    image_info = {
                        "id": f"image_{i}_{j}",  # 生成唯一ID
                        "src": image_data['desktop']['url'],
                        "position": j + 1  # 位置从1开始
                    }
                    images.append(image_info)
                break  # 只需要从一个变体提取图片

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
