from slugify import slugify
from demo3_product.helpers import clean_html_structure, generate_variants_and_options
from demo3_product.spiders.base_spider import RedisBaseSpider



def make_img_list(images):
    new_list = []
    tmp = set()
    po = 0
    for img in images:
        ilinks = [i for i in img.split(" ") if 'https' in i]
        for link in ilinks:
            if link not in tmp:
                po += 1
                tmp.add(link)
                new_list.append({
                    "id": po,
                    "position": po,
                    "src": link
                })
    return new_list

class ThereformationSpider(RedisBaseSpider):

    # Redis Spider 属性
    name = "thereformation"
    redis_key = f"{name}:start_urls"
    allowed_domains = ["thereformation.com", ]

    # 自定义属性
    cid = 1
    user_id = 3
    task_id = 20
    domain = "thereformation.com"


    def make_product_item(self, response, **kwargs):
        meta = response.meta
        description = response.xpath('//div[contains(@class, "pdp__product-description")]').get()
        price = response.xpath('//div[@class="pdp-main"]//span[@class="price--formated"]/text()').extract_first()
        price = float(price.replace("$", "")) if price else None
        colors = response.xpath('//button[contains(@class, "product-attribute__swatch")]/@aria-label').extract()
        colors = [c.split(":")[-1].strip() for c in colors if c and c.strip()]

        sizes = response.xpath('//button[contains(@class, "product-attribute__sizepicker")]/@aria-label').extract()
        sizes = [s.split(":")[-1].strip() for s in sizes if s and s.strip()]

        variants, options = generate_variants_and_options(
            data={
                "colors": {"position": 1, "val": colors},
                "sizes": {"position": 2, "val": sizes},
            },
            default_price=price
        )


        images = response.xpath('//button[contains(@class, "product-gallery__button")]/img/@data-srcset').extract()
        images = make_img_list(images)

        item = {}
        item["title"] = response.xpath('//h1[contains(@class, "pdp__name")]/text()').extract_first()
        item["handle"] = slugify(item["title"])
        item["description"] = clean_html_structure(description)
        item["category"] = meta.get("tags", [])
        item["price"] = price
        item["variants"] = variants
        item["options"] = options

        item['images'] = images

        return item