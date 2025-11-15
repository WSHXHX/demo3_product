import psycopg2
import redis
import json
import time


#
SPIDER_NAME = "jjill"
limittt = 1

# PostgreSQL 配置
# POSTGRES_HOST = "107.150.40.2"
# POSTGRES_PASSWORD = "S4ssbeXn6zeDs8ij"
POSTGRES_HOST = "192.168.1.9"
POSTGRES_PASSWORD = "0000"
POSTGRES_DBNAME = "postgres"
POSTGRES_USER = "postgres"

# Redis 配置
REDIS_HOST = "107.150.40.2"
REDIS_PORT = 6379
REDIS_PASSWORD = "pFKfclD2rU$3lib@6"
REDIS_KEY = f"{SPIDER_NAME}:start_urls"


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
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)
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
        r.lpush(REDIS_KEY, json.dumps(task))
        count += 1

    print(f"✅ 推送 {count} 条任务到 Redis 队列: {REDIS_KEY}")


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