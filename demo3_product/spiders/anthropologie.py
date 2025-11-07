import json
import re
import time
import itertools
from typing import Any

import scrapy
from scrapy.http import Response
from scrapy_redis.spiders import RedisSpider

from demo3_product.items import Demo3ProductItem


class AnthropologieSpider(RedisSpider):
    name = "anthropologie"
    domain = "anthropologie.com"
    task_id = 12
    user_id = 5
    cid = 8
    redis_key = f"{name}:start_urls"
    allowed_domains = ["anthropologie.com",]

    custom_settings = {
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,  # 限制每个域名并发为 4
        "DOWNLOAD_DELAY": 5,  # 每个请求之间延迟 1 秒
    }


    headers = {
        'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language':'zh-CN,zh;q=0.9',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
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
            return scrapy.Request(url=data, callback=self.parse,headers=self.headers)

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

        script = response.xpath('//script[@id="urbnInitialState"]/text()').get()
        res = json.loads(json.loads(script))
        try:
            keys = [_ for _ in list(res.keys()) if _.startswith("product--")]
            key = keys[0]
        except:
            self.logger.error("data error", response.url)
            return

        product = res[key]["core"]["catalogData"]["product"]


        title = product["displayName"]
        handle = product["productSlug"]
        description = product.get("longDescription")

        primarySlice = res[key]["core"]["catalogData"]["skuInfo"]["primarySlice"]
        secondarySlice = res[key]["core"]["catalogData"]["skuInfo"]["secondarySlice"]
        options = [
            {
                "name": primarySlice["displayLabel"],
                "position": 1,
                "values": list(set([i["displayName"] for i in primarySlice["sliceItems"]]))

            },
            {
                "name": secondarySlice["displayLabel"],
                "position": 2,
                "values": list(set([size["displayName"] for size in secondarySlice["sliceItems"][0]["includedSizes"]]))
            }
        ]
        sku = res[key]["core"]["catalogData"]["skuInfo"]
        price = min(sku.get("listPriceHigh", 99999), sku.get("listPriceLow", 99999),
                    sku.get("salePriceHigh", 99999), sku.get("salePriceLow", 99999),)

        images = []
        variants = []
        img_position = 0
        var_position = 0
        for sitem in primarySlice["sliceItems"]:
            iid = sitem["id"]
            fir_img = ""
            for __i, iimg in enumerate(sitem["images"]):
                img_position += 1
                img_slug = f"{iid}_{iimg}"
                if __i == 0:
                    fir_img = img_slug
                images.append({
                    "id": img_slug,
                    "position": img_position,
                    "src": f"https://images.urbndata.com/is/image/Anthropologie/{img_slug}"
                })

            for send in secondarySlice["sliceItems"][0]["includedSizes"]:
                var_position += 1
                send_name = send["displayName"]
                variants.append({
                    "title": f'{sitem["displayName"]} / {send_name}',
                    "price": price,
                    "weght": "",
                    "barcode": "",
                    "curreny": "",
                    "option1": sitem["displayName"],
                    "option2": send_name,
                    "option3": None,
                    "image_id": fir_img,
                    "position": var_position,
                    "weght_unit": "",
                    "compare_at_price": None
                })


        item["task_id"] = self.task_id
        item["user_id"] = self.user_id
        item["cid"] = self.cid
        item["domain"] = self.domain
        item["title"] = title
        item["handle"] = handle
        item["description"] = description
        item["vendor"] = self.name
        item["category"] = json.dumps(meta["tags"])
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