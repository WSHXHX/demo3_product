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
        "ITEM_PIPELINES": {
            "demo3_product.pipelines.CheckExistPipeline": 290,
            "demo3_product.pipelines.MySQLPipeline": 295,
            "demo3_product.pipelines.ElasticsearchPipeline": 300,
            "demo3_product.pipelines.UpdateTaskTableProductNumber": 305,
        },
        "CONCURRENT_REQUESTS": 2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "COOKIES_ENABLED": True,
        "RETRY_ENABLED": False,
        "DOWNLOADER_MIDDLEWARES": {
            'demo3_product.middlewares.DecompressionMiddleware': 543,
            'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': None,
        },
    }

    # 自定义属性
    cid = 1
    user_id = 5
    task_id = 6
    domain = "hellomolly.com"

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
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
