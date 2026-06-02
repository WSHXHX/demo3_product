import json
import time
import itertools

import scrapy
from lxml import etree

from ..items import Demo3ProductItem


class LucyintheskySpider(scrapy.Spider):
    name = "lucyinthesky"
    domain = "lucyinthesky.com"
    task_id = 5
    redis_key = "lucyinthesky:start_urls"

    allowed_domains = ["www.lucyinthesky.com", "media-img.lucyinthesky.com", "api.lucyinthesky.com"]

    def start_requests(self):
        with open(r'C:\Users\XXX\Desktop\mypy\Amazon\Amazon\lucy.html', encoding='utf-8') as f:
            text = f.read()
        tree = etree.HTML(text)

        href  = tree.xpath('//a[@class="d-flex position-relative flzmyw6 f1rm2wf3 f1gd75sv"]/@href')

        for h in href:
            yield scrapy.Request(
                url="https://www.lucyinthesky.com" + h,
                callback=self.parse,
                meta={"tags": ["Summer Dresses", "summer-dresses"]}
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

        product_data = script_data['props']['pageProps']['store']['1ziaih2d']

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
        product_color = script_data['props']['pageProps']['store']['12pc91pg']
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



