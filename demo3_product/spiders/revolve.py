import re
from urllib.parse import urlparse

from slugify import slugify

import scrapy

from demo3_product.helpers import clean_html_structure, generate_variants_and_options
from demo3_product.spiders.base_spider import RedisBaseSpider



def make_img_list(images):
    new_list = []
    tmp = set()
    po = 0
    for img in images:
        if img not in tmp:
            po += 1
            tmp.add(img)
            new_list.append({
                "id": po,
                "position": po,
                "src": img
            })
    return new_list

def get_price(price_list):
    pl = []
    for p in price_list:
        try: pl.append(float(p.replace("$", "")))
        except: pass
    if not pl: return 88.8
    return min(pl)

def make_title(title_list):
    for t in title_list:
        if t and t.strip():
            return t.strip()


class RevolveSpider(RedisBaseSpider):

    # Redis Spider 属性
    name = "revolve"
    redis_key = f"{name}:start_urls"
    allowed_domains = ["revolve.com", ]

    # 自定义属性
    cid = 1
    user_id = 6
    task_id = 27
    domain = "revolve.com"

    # custom_settings = {"ITEM_PIPELINES": {"demo3_product.pipelines.CheckExistPipeline": 290, },}
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
        'accept_language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
    }

    def make_product_item(self, response, **kwargs):
        meta = response.meta
        title_list = response.xpath('//div[@class="product-titles"]/div/h1/text()').getall()

        price = response.xpath('//span[@id="retailPrice"]/text()').getall()
        price = get_price(price)

        colors = response.xpath('//ul[@id="product-swatches"]/li/@data-color-name').getall()
        if not colors:
            colors = response.xpath('//span[@aria-live="polite"]/text()').getall()
        colors = [c.strip() for c in colors if c and c.strip()]

        sizes = response.xpath('//ul[@id="size-ul"]/li/input/@data-size-value').getall()
        sizes = [s.strip() for s in sizes if s and s.strip()]

        dd = {}
        if colors: dd.update({"colors": {"position": len(dd) + 1, "val": colors}})
        if sizes: dd.update({"sizes": {"position": len(dd) + 1, "val": sizes}})
        variants, options = generate_variants_and_options(data=dd, default_price=price)


        images = response.xpath('//div[@id="model-1-images"]/button/@data-image').getall()
        images = make_img_list([i.split("?")[0] for i in images])

        item = {}
        item["title"] = make_title(title_list)
        item["handle"] = slugify(item["title"])
        item["category"] = meta.get("tags", [])
        item["price"] = price
        item["variants"] = variants
        item["options"] = options
        item['images'] = images
        item['description'] = ''
        return item

    def parse(self, response, **kwargs):
        item = self.parse_product(response)
        up = urlparse(response.url)
        d = pa = ''
        pa = up.path.strip('/').split('/')[-1]
        for q in up.query.split('&'):
            if q.startswith('d='):
                d = q[2:]
        if d and pa:
            headers = self.headers.copy()
            headers.update({"referer": response.url})
            url = f"https://www.revolve.com/content/product/getMarkup/productDetailsTab/{pa}?d={d}&code={pa}"
            yield scrapy.Request(
                url=url,
                callback=self.parse_detail,
                meta={'item': item},
                headers=headers,
            )

    def parse_detail(self, response, **kwargs):
        item = response.meta['item']
        description = response.xpath('//div[contains(@class, "product-details__description")]').get()
        item["description"] = clean_html_structure(description)
        yield item