import re
import json

import scrapy
from demo3_product.spiders.base_spider import RedisBaseSpider
from demo3_product.helpers import generate_variants_and_options


class MonclerSpider(RedisBaseSpider):
    cid = 2
    name = 'moncler'
    domain = 'moncler.com'
    task_id = 39
    user_id = 6
    redis_key = 'moncler:start_urls'
    allowed_domains = ['moncler.com']

    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
    }

    def parse(self, response, **kwargs):
        links = re.findall("<loc>(https://www.moncler.com/en-us/.*?)</loc>", response.text, re.DOTALL)
        for link in links:
            yield scrapy.Request(
                url=link,
                headers=self.headers,
                callback=self.parse_product
            )

    def make_product_item(self, response, **kwargs):
        data_text = response.xpath("//script[@id='mobify-data']/text()").get()
        data_json = json.loads(data_text)


        products = data_json["__PRELOADED_STATE__"]["__STATE_MANAGEMENT_LIBRARY"]["store"]["productStore"]["productsById"].values()
        categories = data_json["__PRELOADED_STATE__"]["__STATE_MANAGEMENT_LIBRARY"]["store"]["categoryStore"]["categories"]

        for product in products:
            data_res = {}
            data_res['title'] = product["name"]
            data_res['handle'] = product["selectedProductUrl"].replace(".html", "")
            data_res['description'] = product["longDescription"]
            data_res['price'] = product["price"]["sales"]["value"]

            cate_id = product["primaryCategoryId"]
            if categories.get(cate_id):
                cate_tree = categories[cate_id]["parentCategoryTree"]
                data_res['category'] = [_["id"] for _ in cate_tree] + [_["name"] for _ in cate_tree]
            else:
                data_res['category'] = [cate_id]

            images = product["images"]
            data_res['images'] = [{
                "id": po + 1,
                "position": po + 1,
                "src": img["url"].replace("\u002F", "/"),
            } for po, img in enumerate(images)]
            vs = product["variants"]
            colors = [v["variationValues"]["color"] for v in vs]
            sizes = [v["variationValues"]["size"] for v in vs]

            dd = {}
            if colors: dd.update({"colors": {"position": len(dd) + 1, "val": colors}})
            if sizes: dd.update({"sizes": {"position": len(dd) + 1, "val": sizes}})
            variants, options = generate_variants_and_options(data=dd, default_price=data_res['price'])
            data_res['variants'] = variants
            data_res['options'] = options
            return data_res




