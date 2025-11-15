import os
import json
import time
from typing import Any

from urllib.parse import urlparse
from scrapy.http import Response

from demo3_product.items import Demo3ProductItem
from demo3_product.spiders.base_spider import RedisBaseSpider


class JjillSpider(RedisBaseSpider):
    name = "jjill"
    domain = "jjill.com"
    task_id = 15
    user_id = 3
    cid = 1
    redis_key = f"{name}:start_urls"
    allowed_domains = ["jjill.com"]

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'sec-ch-ua': '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    }

    def parse(self, response: Response, **kwargs: Any) -> Any:
        meta = response.meta
        item = Demo3ProductItem()
        nnow = int(time.time())
        print("--",response)
        with open(r"C:\Users\XXX\AppData\Roaming\JetBrains\PyCharm2025.2\scratches\scratch_1.html", "w", encoding="utf-8") as f:
            f.write(response.text)
        script = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        if not script:
            script = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        res = json.loads(script)

        product_data = res["props"]["pageProps"]["pageData"]["product"]

        title = product_data.get("nameText", "")
        handle = product_data.get("url", "").replace("/product/", "")
        description = product_data.get("description", "").replace("\u003c", "<").replace("\u003e", ">")
        price = product_data.get("price", "0.00")

        variants = []
        varposition = 0
        for v in product_data["variants"]:
            varposition += 1
            title = ""
            if v.get("color", None): title = v["color"]["text"]
            if v.get("size", None): title = title + " / " + v["size"]["text"]
            if v.get("extendedSize", None): title = title + " / " + v["extendedSize"]["text"]

            variants.append({
                "title": title,
                "price": v.get("price", "0.00"),
                "weght": "",
                "barcode": "",
                "curreny": "",
                "option1": v.get("color", None),
                "option2": v.get("size", None),
                "option3": v.get("extendedSize", None),
                "image_id": v.get("firstImageCode", None),
                "position": varposition,
                "weght_unit": "",
                "compare_at_price": None
            })

        options = []
        opt1 = [_v.get("option1", None) for _v in variants]
        opt2 = [_v.get("option2", None) for _v in variants]
        opt3 = [_v.get("option3", None) for _v in variants]
        opt1 = [_ for _ in opt1 if _]
        opt2 = [_ for _ in opt2 if _]
        opt3 = [_ for _ in opt3 if _]

        if opt1:
            options.append({
                "name": "color",
                "position": len(variants) + 1,
                "values": opt1
            })
        if opt2:
            options.append({
                "name": "size",
                "position": len(variants) + 1,
                "values": opt2
            })
        if opt3:
            options.append({
                "name": "extendedSize",
                "position": len(variants) + 1,
                "values": opt3
            })

        images = []
        piid = 0
        iimgs = product_data["media"]["normalizedMedia"]
        for iimg in iimgs.values():
            for _i in iimg["fullImages"]:
                piid += 1
                isrc = _i["src"]
                img_slug = os.path.splitext(os.path.basename(urlparse(isrc).path))[0]
                images.append({
                    "id": img_slug,
                    "position": piid,
                    "src": isrc
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

