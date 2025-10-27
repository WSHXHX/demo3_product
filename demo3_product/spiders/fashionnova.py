import json
import time
import itertools


import psycopg2
from lxml import html

import scrapy
from scrapy import signals
from scrapy.exceptions import DontCloseSpider

from demo3_product.items import Demo3ProductItem




def read_line():
    conn = psycopg2.connect(
        host="192.168.1.32",
        dbname="postgres",
        user="postgres",
        password="0000"
    )
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
                UPDATE spider_temp
                SET status = 10
                WHERE id IN (
                    SELECT id FROM spider_temp
                    WHERE status = 1
                    LIMIT 100
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, link, tags, referer;
            """)
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return rows


class FashionnovaSpider(scrapy.Spider):
    name = "fashionnova"
    domain = "fashionnova.com"
    offset = 0
    is_finished = False
    allowed_domains = ["www.fashionnova.com"]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(FashionnovaSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def start_requests(self):
        yield from self.load_next_batch()

    def load_next_batch(self):
        rows = read_line()
        if not rows:
            self.is_finished = True
            return []
        self.offset += len(rows)
        self.logger.info(f"加载第 {self.offset // 100} 批数据，共 {len(rows)} 条")
        for id_, link, tags, referer in rows:
            yield scrapy.Request(
                link,
                callback=self.parse_page,
                headers={'Referer': referer},
                meta={'postid': id_, 'tags': tags}
            )

    def spider_idle(self):
        """当队列空了后，再加载下一批"""
        if not self.is_finished:
            self.logger.info("当前批次爬完，加载下一批数据...")
            new_requests = list(self.load_next_batch())
            if new_requests:
                for req in new_requests:
                    self.crawler.engine.crawl(req)
                # 阻止 Scrapy 关闭
                raise DontCloseSpider
            else:
                self.logger.info("没有更多任务了，爬虫结束。")

    def parse_page(self, response):
        meta = response.meta
        item = Demo3ProductItem()
        nnow = int(time.time())

        div = response.xpath('//div[@data-testid="accordion-children-container"]').get()
        element = html.fromstring(div)
        for el in element.xpath('.//*'):
            el.attrib.clear()
        clean_html = html.tostring(element, encoding='unicode', method='html')

        data = {}
        for script in response.xpath('//script[@type="application/ld+json"]/text()').getall():
            try:
                data = json.loads(script)
                if data.get('@type') == 'Product':
                    break
            except json.JSONDecodeError:
                pass
        iimages = [_.split('&')[0] for _ in  response.xpath('//div[@data-testid="pdp-image-gallery-grid"]//img/@src').getall()]
        nimages = list(set(iimages))
        nimages.sort(key=lambda x: iimages.index(x))
        images = [{"id": _i + 1, "src": i, "position": _i + 1} for _i, i in enumerate(nimages)]
        title = response.xpath('//h1[@data-testid="product-title"]/text()').extract_first()
        price = response.xpath('//div[@data-testid="product-price-regular"]/div/text()').extract_first()
        if data:
            title = data['name']
            price = data['offers']['price']

        color_options = []
        for c in response.xpath('//div[starts-with(@data-testid, "swatch-option-")]'):
            color_title = c.xpath('./@title').get()
            color_options.append(color_title.split('color')[-1].strip())
        color_options = list(set(color_options))

        # 2️⃣ 提取尺码
        size_options = []
        for s in response.xpath('//div[@data-testid="product-size-options"]//button'):
            label = s.xpath('.//div/text()').get(default='').strip()
            size_options.append(label)

        size_options = list(set(size_options))
        options = []

        if color_options and size_options:
            variants = [
                {
                    "title": f"{c} / {s}",
                    "price": str(price),
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
                for __i, (c, s) in enumerate(itertools.product(color_options, size_options))
            ]
            options.append({
                "name": "color",
                "values": color_options,
                "position": 1
            })
            options.append({
                "name": "size",
                "values": size_options,
                "position": 2
            })
        elif color_options:
            variants = [
                {
                    "title": f"{c}",
                    "price": str(price),
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
                for __i, c in enumerate(color_options)
            ]
            options.append({
                "name": "color",
                "values": color_options,
                "position": 1
            })
        elif size_options:
            variants = [
                {
                    "title": f"{s}",
                    "price": str(price),
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
                for __i, s in enumerate(size_options)
            ]
            options.append({
                "name": "size",
                "values": size_options,
                "position": 1
            })
        else:
            variants = []

        item["task_id"] = 4
        item["user_id"] = 10
        item["cid"] = 1
        item["domain"] = 'fashionnova.com'
        item["title"] = title
        item["handle"] = response.url.split('/')[-1]
        item["description"] = clean_html
        item["vendor"] = 'fashionnova'
        item["category"] = json.dumps(meta.get("tags"))
        item["original_price"] = float(price)
        item["current_price"] = float(price)
        item["images"] = json.dumps(images)
        item["variants"] = json.dumps(variants)
        item["tags"] = json.dumps(meta.get("tags"))
        item["created_at"] = nnow
        item["updated_at"] = nnow
        item["type"] = 1
        item["platform"] = 4
        item["options"] = json.dumps(options)
        item["postid"] = meta.get("postid")
        self.logger.info(f"✅ get product item: {title}")
        yield item

