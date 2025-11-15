import json

import scrapy
from scrapy_redis.spiders import RedisSpider


class RedisBaseSpider(RedisSpider):

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