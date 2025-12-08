import time
import json

import scrapy
from scrapy_redis.spiders import RedisSpider
from demo3_product.items import Demo3ProductItem


class RedisBaseSpider(RedisSpider):
    cid = None
    name = None
    domain = None
    task_id = None
    user_id = None
    redis_key = None
    allowed_domains = None

    required_class_attrs = ["cid", "domain", "task_id", "user_id", "allowed_domains", "redis_key", "name"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 检查子类是否覆盖了属性
        for attr in self.required_class_attrs:
            val = getattr(self, attr, None)
            if val is None:
                raise NotImplementedError(
                    f"{self.__class__.__name__} must override class attribute `{attr}`"
                )

    def make_request_from_data(self, data):
        """
        scrapy-redis 默认只支持 URL 字符串
        我们重写这个方法，让它能解析 JSON
        """

        # spider 类里定义的 headers（如果没有就返回 {}）
        class_headers = getattr(self, "headers", {})

        if isinstance(data, bytes):
            try:
                data = data.decode("utf-8")
            except UnicodeDecodeError:
                # 不能 UTF-8 解码 → 当作普通 URL 处理
                return scrapy.Request(url=data.decode("latin-1"), callback=self.parse, headers=class_headers)

        try:
            task = json.loads(data)
        except json.JSONDecodeError:
            # 如果不是 JSON，就当成 URL 字符串
            return scrapy.Request(url=data, callback=self.parse, headers=class_headers)

        url = task.get("url")
        # task 自己的 headers
        task_headers = task.get("headers", {})

        # 合并，task 的 headers 优先覆盖类的 headers
        headers = {**class_headers, **task_headers}

        meta = task.get("meta", {})

        return scrapy.Request(
            url=url,
            headers=headers,
            meta=meta,
            callback=self.parse
        )

    def to_str(self, item):
        if type(item) == str:
            return item
        else:
            return json.dumps(item)

    def parse(self, response, **kwargs):
        yield self.parse_product(response, **kwargs)

    def parse_product(self, response, **kwargs):
        meta = response.meta
        item = Demo3ProductItem()
        nnow = int(time.time())

        product_item = self.make_product_item(response, **kwargs)

        item["task_id"] = self.task_id
        item["user_id"] = self.user_id
        item["cid"] = self.cid
        item["domain"] = self.domain
        item["title"] = product_item["title"].strip()
        item["handle"] = product_item["handle"].strip()
        item["description"] = product_item["description"]
        item["vendor"] = self.name
        item["category"] = self.to_str(product_item["category"])
        item["original_price"] = float(product_item["price"])
        item["current_price"] = float(product_item["price"])
        item["images"] = self.to_str(product_item["images"])
        item["variants"] = self.to_str(product_item["variants"])
        item["tags"] = '[]'
        item["created_at"] = nnow
        item["updated_at"] = nnow
        item["type"] = 1
        item["platform"] = 4
        item["options"] = self.to_str(product_item["options"])
        item["postid"] = meta.get("postid")
        self.logger.info(f" ➜ [F] Fetch product item: {product_item['title'].strip()}")
        return item

    def make_product_item(self, response, **kwargs):
        raise NotImplementedError(f"{self.__class__.__name__} must override class function `make_product_item()`")
