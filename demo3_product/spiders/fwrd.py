import time
import json
import re

import scrapy
from scrapy_redis.spiders import RedisSpider
from demo3_product.items import Demo3ProductItem
from demo3_product.helpers import clean_html_structure, generate_variants_and_options

class FwrdSpider(RedisSpider):
    cid = 1
    user_id = 3

    name = "fwrd"
    domain = "fwrd.com"
    task_id = 49
    redis_key = f"{name}:start_urls"
    allowed_domains = ["fwrd.com"]

    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-user': '?1',
    }
    cheaders = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
        'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
    }
    pheaders = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
        'sec-ch-ua': '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
    }

    def make_request_from_data(self, data):
        try:
            nd = data.decode("utf-8")
        except:
            nd = data.decode("latin-1")

        return scrapy.Request(
            url=nd,
            callback=self.parse,
            headers=self.headers,
            dont_filter=True,
        )


    def parse(self, response, **kwargs):
        rrr = re.findall("<loc>(.*?)</loc>", response.text)
        for r in rrr:
            yield scrapy.Request(
                url=r,
                headers=self.headers,
                callback=self.parse_xml,
                dont_filter=True,
            )

    def parse_xml(self, response, **kwargs):
        rrr = re.findall("<loc>(.*?)</loc>", response.text)
        for r in rrr:
            yield scrapy.Request(
                url=r,
                headers=self.cheaders,
                callback=self.parse_page,
                dont_filter=True,
            )

    def parse_page(self, response, **kwargs):

        meta = response.meta
        if meta.get("tags"):
            tags = meta.get("tags")
        else:
            tags = []
            tags += response.xpath('//h1[@class="page-titles__plp-hed "]/text()').getall()
            tags += response.xpath('//li[@property="itemListElement"]/a/@title').getall()


        products_hrefs = response.xpath('//a[@class="product-grids__link product__image-alt-trigger js-plp-pdp-link u-relative"]/@href').getall()
        for products_href in products_hrefs:
            yield scrapy.Request(
                url="https://www.fwrd.com" + products_href,
                headers=self.pheaders,
                callback=self.parse_product,
                meta={"tags": tags},
            )


        lazy_load_url = response.xpath(
            '//ul[@class="g g--collapse n-block-grid--3 product-grids js-plp-lazy-load"]/@data-lazy-load-url').get()

        yield scrapy.Request(
            url="https://www.fwrd.com" + lazy_load_url,
            headers=self.cheaders,
            callback=self.parse_page,
            dont_filter=True,
            meta={"tags": tags},
        )

    def parse_product(self, response, **kwargs):
        meta = response.meta
        tags = meta.get("tags")


        title = response.xpath('//div[@class="pdp__brand-desc u-capitalize"]/text()').get()
        handle = response.url.split('/')[3]
        price = response.xpath('//span[@class="price__retail" or @class="price__sale"]/text()').get().replace(',', '')

        description = response.xpath('//div[@id="pdp-details"]').get()

        colors = response.xpath('//span[@class="pdp__color-option"]/text()').getall()
        sizes = response.xpath('//label[@class="pdp__size-push-button push-button push-button--sm u-margin-b--none"]').getall()

        images = response.xpath("//img[contains(concat(' ', normalize-space(@class), ' '), ' pdp__image ') and contains(concat(' ', normalize-space(@class), ' '), ' u-aspect-66 ')]/@src").getall()

        images = [{
                "id": po + 1,
                "position": po + 1,
                "src": img,
            } for po, img in enumerate(images)]

        dd = {}
        if colors: dd.update({"colors": {"position": len(dd) + 1, "val": colors}})
        if sizes: dd.update({"sizes": {"position": len(dd) + 1, "val": sizes}})
        variants, options = generate_variants_and_options(data=dd, default_price=price)

        item = Demo3ProductItem()
        nnow = int(time.time())

        item["type"] = 1
        item["tags"] = '[]'
        item["platform"] = 4
        item["cid"] = self.cid
        item["created_at"] = nnow
        item["updated_at"] = nnow
        item["vendor"] = self.name
        item["domain"] = self.domain
        item["task_id"] = self.task_id
        item["user_id"] = self.user_id

        item["title"] = title
        item["handle"] = handle
        item["description"] = clean_html_structure(description)
        item["category"] = self.to_str(tags)
        item["original_price"] = float(price.replace('$', ''))
        item["current_price"] = float(price.replace('$', ''))
        item["images"] = self.to_str(images)
        item["variants"] = self.to_str(variants)
        item["options"] = self.to_str(options)

        yield item

    def to_str(self, item):
        if type(item) == str:
            return item
        else:
            return json.dumps(item)