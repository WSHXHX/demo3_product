import os
import sys
import time
import json

import redis
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from demo3_product.settings import POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_DBNAME, POSTGRES_USER, REDIS_HOST, REDIS_PORT, REDIS_PARAMS

############### Task Config #########################

SPIDER_NAME = "revolve"
limittt = 100

#####################################################

def read_line(limit=100):
    """
    从 PostgreSQL 取出 100 条任务，更新状态为 10，并返回 (id, link, tags, referer)
    """
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DBNAME,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
        UPDATE spider_temp
        SET status = 10
        WHERE id IN (
            SELECT id FROM spider_temp
            WHERE status = 1 AND domain=%s
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, link, tags, referer;
    """, (SPIDER_NAME, limit))
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return rows

def push_to_redis(rows):
    """
    把任务推送到 Redis 队列（scrapy-redis）
    """
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PARAMS["password"])
    count = 0

    for id_, link, tags, referer in rows:
        task = {
            "url": link,
            "meta": {
                "postid": id_,
                "tags": tags,
                "referer": referer
            },
            "headers": {
                "Referer": referer or f"https://www.{SPIDER_NAME}.com/"
            }
        }
        r.lpush(f"{SPIDER_NAME}:start_urls", json.dumps(task))
        count += 1

    print(f"✅ 推送 {count} 条任务到 Redis 队列: {f'{SPIDER_NAME}:start_urls'}")


def main():
    """
    主流程：循环读取数据库任务并推送到 Redis
    """
    while True:
        rows = read_line(limittt)
        if not rows:
            print("✅ 已推送完成")
            time.sleep(30)
            break
        push_to_redis(rows)
        if limittt == 1:
            break


if __name__ == "__main__":
    main()
