from slugify import slugify
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
        try: pl.append(float(p))
        except: pass
    if not pl: return 88.8
    return min(pl)

class ShowpoSpider(RedisBaseSpider):

    # Redis Spider 属性
    name = "showpo"
    redis_key = f"{name}:start_urls"
    allowed_domains = ["showpo.com", ]

    # 自定义属性
    cid = 1
    user_id = 3
    task_id = 24
    domain = "showpo.com"

    custom_settings = {"ITEM_PIPELINES": {"demo3_product.pipelines.CheckExistPipeline": 290, },}


    def make_product_item(self, response, **kwargs):
        meta = response.meta
        description = response.xpath('//div[contains(@class, "shopify-html-content")]').get()
        price = response.xpath('//div[@data-testid="product-price"]//span/text()').getall()
        price = get_price(price)

        colors = response.xpath('//div[@class="overflow-hidden"]/div/div/a/@aria-label').getall()
        colors = [c.replace("View in", "").strip() for c in colors if c and c.strip()]

        sizes = response.xpath('//div[@class="grid grid-cols-4 gap-1"]/a/@aria-label').getall()
        sizes = [s.replace("Size", "").strip() for s in sizes if s and s.strip()]

        dd = {}
        if colors: dd.update({"colors": {"position": len(dd) + 1, "val": colors}})
        if sizes: dd.update({"sizes": {"position": len(dd) + 1, "val": sizes}})
        variants, options = generate_variants_and_options(data=dd, default_price=price)


        images = response.xpath('//div[@aria-label="Product Image"]/div/img/@src').extract()
        images = make_img_list([i.split("?")[0] for i in images])

        item = {}
        item["title"] = response.xpath('//h1[contains(@data-testid, "product-title")]/text()').extract_first()
        item["handle"] = slugify(item["title"])
        item["description"] = clean_html_structure(description)
        item["category"] = meta.get("tags", [])
        item["price"] = price
        item["variants"] = variants
        item["options"] = options

        item['images'] = images

        return item