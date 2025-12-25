import os
import sys
import json

import redis

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from demo3_product.settings import REDIS_HOST, REDIS_PORT, REDIS_PARAMS


if __name__ == '__main__':

    START_URL = "https://www.moncler.com/en-us/sitemap_0-product.xml"
    SPIDER_NAME = "moncler"

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PARAMS["password"])
    task = {
        "url": START_URL,
    }
    r.lpush(f"{SPIDER_NAME}:start_urls", json.dumps(task))

    print(f"✅ 推送 {START_URL} 到 Redis 队列: {f'{SPIDER_NAME}:start_urls'}")
