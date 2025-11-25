import json
from typing import Any

from scrapy.http import Response
from demo3_product.spiders.base_spider import RedisBaseSpider


class HelloMollySpider(RedisBaseSpider):

    # Redis Spider 属性
    name = "hellomolly"
    redis_key = f"{name}:start_urls"
    allowed_domains = ["www.hellomolly.com", "searchspring.io"]
    custom_settings = {
        "ITEM_PIPELINES": {"demo3_product.pipelines.CheckExistPipeline": 290,},
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': 400,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': None,
        }
    }

    # 自定义属性
    cid = 1
    user_id = 5
    task_id = 6
    domain = "hellomolly.com"

    handle_httpstatus_list = [429]
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        "accept-encoding": "gzip, deflate, br, zstd",
        "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1"
    }


    def make_product_item(self, response: Response, **kwargs: Any) -> Any:

        print(response.text)
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

        price = float(product_data['price']['amount'])
        if not price:
            price = float(product_data['compareAtPrice']['amount'])


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

        item = {}
        item["title"] = product_data['title']
        item["handle"] = product_data['handle']
        item["description"] = product_data['descriptionHtml'].replace('\u003c', '<').replace('\u003e', '>')
        item["category"] = product_data['tags']
        item["price"] = price
        item["variants"] = variants
        item["options"] = options

        item['images'] = images

        return item
