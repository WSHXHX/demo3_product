import json
import time

from typing import Any

from lxml import html

import scrapy
from scrapy.http import Response
from scrapy_redis.spiders import RedisSpider

from demo3_product.items import Demo3ProductItem
from demo3_product.helpers import generate_variants_and_options

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
                if 'productDataPreload' in script:
                    script_data = json.loads(script.split(':', 1)[1][:-5].replace('\\"', '"').replace('\\\\', '\\'))
                    break
            except json.JSONDecodeError as e:
                pass
            except IndexError as e:
                pass
        else:
            self.logger.error(f"没找到数据: {response.url}")
            return

        for s in script_data:
            if type(s) == dict:
                product_data = script_data[3]
                break
        else:
            self.logger.warning(f"not data: {response.url}")
            return

        title = product_data["productDataPreload"].get("title1", "") + product_data["productDataPreload"].get("title2", "")
        handle = product_data['slugParam']
        description_node = response.xpath('//div[@class=" hidden w-full lg:flex flex-col gap-[30px]"]').get()

        element = html.fragment_fromstring(description_node, create_parent=True)
        for el in element.iter(): el.attrib.clear()

        description = html.tostring(element, encoding='unicode', method='html')
        description = description.replace('\\r', '\r').replace('\\n', '\n').replace('\xa0', '&nbsp;')


        colors = product_data['productDataPreload']['colors']
        ss = product_data['productDataPreload']['sizes']
        if type(ss) == dict:
            sizes = [sss["name"] for s in ss.values() for sss in s]
        elif type(ss) == list:
            sizes = [s["name"] for s in ss]
        price = product_data['productDataPreload']['rawPriceCurrency']

        variants, options = generate_variants_and_options(colors, sizes)

        category = meta.get("tags")
        category = list(set(category))

        images_links = product_data['productDataPreload']['media']['images']
        images = [
            {
                "id": __i + 1,
                "scr": "https://d166chel5lrjm5.cloudfront.net/images" + img["desktop"],
                "position": __i + 1
            } for __i, img in enumerate(images_links) if img.get("desktop", "")
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
