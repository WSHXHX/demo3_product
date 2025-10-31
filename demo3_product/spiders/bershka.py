import json
import re
import time
import itertools
from typing import Any

import scrapy
from scrapy.http import Response
from scrapy_redis.spiders import RedisSpider

from demo3_product.items import Demo3ProductItem


def generate_variants_and_options(colors=None, sizes=None, lengths=None):
    # 准备列表和对应的名称、位置
    lists_info = []
    if colors:
        lists_info.append(("color", colors, len(lists_info) + 1))
    if sizes:
        lists_info.append(("size", sizes, len(lists_info) + 1))
    if lengths:
        lists_info.append(("length", lengths, len(lists_info) + 1))

    # 如果没有列表，返回空结果
    if not lists_info:
        return [], []

    # 提取列表用于组合
    lists_for_combination = [info[1] for info in lists_info]

    # 生成所有组合
    combinations = list(itertools.product(*lists_for_combination))

    # 生成变体列表
    variants = []
    for combo in combinations:
        item = {}
        title_parts = []

        for i, (name, values, position) in enumerate(lists_info):
            item[f"option{i + 1}"] = combo[i]
            title_parts.append(str(combo[i]))

        item["title"] = " / ".join(title_parts)
        variants.append(item)

    # 生成选项配置
    options = []
    for name, values, position in lists_info:
        options.append({
            "name": name,
            "values": values,
            "position": position
        })

    return variants, options



class BershkaSpider(RedisSpider):
    name = "bershka"
    domain = "bershka.com"
    task_id = 11
    redis_key = f"{name}:start_urls"
    allowed_domains = ["bershka.com",]

    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        # 'sec-fetch-user': '?1',
    }

    def make_request_from_data(self, data):
        """
        scrapy-redis 默认只支持 URL 字符串
        我们重写这个方法，让它能解析 JSON
        """
        if isinstance(data, bytes):
            try:
                data = data.decode("utf-8")
            except UnicodeDecodeError:
                # 不能 UTF-8 解码 → 当作普通 URL 处理
                return scrapy.Request(url=data.decode("latin-1"), callback=self.parse,headers=self.headers)

        try:
            task = json.loads(data)
        except json.JSONDecodeError:
            # 如果不是 JSON，就当成 URL 字符串
            return scrapy.Request(url=data, callback=self.parse,headers=self.headers)

        url = task.get("url")
        headers = task.get("headers", {})
        headers.update(self.headers)
        meta = task.get("meta", {})

        return scrapy.Request(
            url=url,
            headers=headers,
            meta=meta,
            callback=self.parse
        )

    def parse(self, response: Response, **kwargs: Any) -> Any:
        meta = response.meta
        item = Demo3ProductItem()
        nnow = int(time.time())

        title = response.xpath('//h1[@class="product-detail-info-layout__title bds-typography-heading-xs"]/text()').extract_first().strip()
        handle = response.url.split('?')[0].split('/')[-1].replace('.html', '')

        description = response.xpath('//meta[@name="description"]/@content').extract_first()
        isrc = response.xpath('//img[@data-qa-anchor="pdpMainImage"]/@src').getall()
        images = [{
            "id": _i + 1,
            "src": src,
            "position": _i + 1,
        } for _i, src in enumerate(isrc) ]
        prices = re.findall(r'"price":"(\d+.\d+)"', response.text)
        price = min(prices)

        colors = response.xpath('//ul[@data-qa-anchor="productDetailColorList"]//img/@alt').getall()
        sizes = response.xpath('//button[@data-qa-anchor="sizeListItem"]//text()').getall()
        lengths = response.xpath('//button[@data-qa-anchor="productDetailSize"]//text()').getall()

        colors = [c for c in  colors if c and c.strip()]
        sizes = [s for s in sizes if s and s.strip()]
        lengths = [l for l in lengths if l and l.strip()]

        variants, options = generate_variants_and_options(colors, sizes, lengths)

        item["task_id"] = self.task_id
        item["user_id"] = 3
        item["cid"] = 1
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
