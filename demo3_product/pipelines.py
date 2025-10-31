# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
import json
import urllib3

import pymysql
import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from scrapy.exceptions import NotConfigured

from itemadapter import ItemAdapter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Demo3ProductPipeline:
    def process_item(self, item, spider):
        return item


class MySQLPipeline:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host=crawler.settings.get("MYSQL_HOST"),
            port=crawler.settings.getint("MYSQL_PORT"),
            user=crawler.settings.get("MYSQL_USER"),
            password=crawler.settings.get("MYSQL_PASSWORD"),
            database=crawler.settings.get("MYSQL_DB"),
        )

    def open_spider(self, spider):
        # 连接数据库
        self.conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset="utf8mb4"
        )
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()

    def process_item(self, item, spider):
        data = ItemAdapter(item).asdict()
        # 示例：插入表 product_data，可根据你的表结构修改
        sql = """
        INSERT INTO collection_products (task_id, user_id, cid, domain, title, handle, description, vendor, category, original_price, current_price, images, variants, tags, created_at, updated_at, type, platform, options)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            self.cursor.execute(sql, (
                data.get("task_id"),
                data.get("user_id"),
                data.get("cid"),
                data.get("domain"),
                data.get("title"),
                data.get("handle"),
                data.get("description"),
                data.get("vendor"),
                data.get("category"),
                data.get("original_price"),
                data.get("current_price"),
                data.get("images"),
                data.get("variants"),
                data.get("tags"),
                data.get("created_at"),
                data.get("updated_at"),
                data.get("type"),
                data.get("platform"),
                data.get("options"),
            ))
            self.conn.commit()

            handle = data.get("handle")
            nnow = data.get("created_at")
            self.cursor.execute(f"SELECT id FROM collection_products WHERE handle='{handle}' AND created_at='{nnow}'")
            ids = self.cursor.fetchall()
            item["mysqlid"] = ids[0][0]
            spider.logger.info(f'✅ Insert item to MySQL, id: {item["mysqlid"]}, title: {data.get("title")}')
        except Exception as e:
            spider.logger.error(f"Insert error: {e}")
            self.conn.rollback()
        return item


class ElasticsearchPipeline:
    def __init__(self, es_host, es_user, es_pass, index_name):
        self.es_host = es_host
        self.es_user = es_user
        self.es_pass = es_pass
        self.index_name = index_name
        self.es = None
        self.buffer = []

    @classmethod
    def from_crawler(cls, crawler):
        es_host = crawler.settings.get("ES_HOST")
        es_user = crawler.settings.get("ES_USER")
        es_pass = crawler.settings.get("ES_PASS")
        index_name = crawler.settings.get("INDEX_NAME")

        if not all([es_host, es_user, es_pass, index_name]):
            raise NotConfigured("Elasticsearch settings are incomplete")

        return cls(es_host, es_user, es_pass, index_name)

    def open_spider(self, spider):
        """连接 Elasticsearch"""
        self.es = Elasticsearch(
            [self.es_host],
            basic_auth=(self.es_user, self.es_pass),
            verify_certs=False,
            ssl_show_warn=False,
        )

    def process_item(self, item, spider):
        doc = {
            "id": item["mysqlid"],
            "task_id": item["task_id"],
            "user_id": item["user_id"],
            "cid": item["cid"],
            "domain": item["domain"],
            "title": item["title"],
            "handle": item["handle"],
            "vendor": item["vendor"],
            "category": ",".join(json.loads(item["category"])),
            "original_price": item["original_price"],
            "current_price": item["current_price"],
            "tags": ",".join(json.loads(item["tags"])),
            "created_at": item["created_at"],
            "updated_at": item["updated_at"],
            "type": item["type"],
            "platform": item["platform"]
        }
        self.es.index(index=self.index_name, id=doc["id"], document=doc)
        return item

    def flush(self):
        if self.buffer:
            bulk(self.es, self.buffer)
            self.buffer.clear()

    def close_spider(self, spider):
        self.flush()


class PostgresUpdatePipeline:
    def __init__(self, host, dbname, user, password):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None

    @classmethod
    def from_crawler(cls, crawler):
        host = crawler.settings.get("POSTGRES_HOST")
        dbname = crawler.settings.get("POSTGRES_DBNAME")
        user = crawler.settings.get("POSTGRES_USER")
        password = crawler.settings.get("POSTGRES_PASSWORD")

        if not all([host, dbname, user, password]):
            raise NotConfigured("PostgreSQL settings are incomplete")

        return cls(host, dbname, user, password)

    def open_spider(self, spider):
        """在爬虫启动时连接数据库"""
        self.conn = psycopg2.connect(
            host=self.host,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        """根据 item['postid'] 更新状态"""
        postid = item.get("postid")
        if postid:
            try:
                self.cursor.execute(
                    "UPDATE public.spider_temp SET status = 2 WHERE id = %s;",
                    (postid,)
                )
                self.conn.commit()
                spider.logger.info(f"✅ Postgres 更新成功: id={postid}")
            except Exception as e:
                self.conn.rollback()
                spider.logger.error(f"❌ Postgres 更新失败: id={postid}, error={e}")
        else:
            spider.logger.warning("⚠️ Item 没有 postid，跳过 Postgres 更新")

        return item  # 一定要返回 item 让后续管道能继续用

    def close_spider(self, spider):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


class UpdateImagesPipline:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host=crawler.settings.get("MYSQL_HOST"),
            port=crawler.settings.getint("MYSQL_PORT"),
            user=crawler.settings.get("MYSQL_USER"),
            password=crawler.settings.get("MYSQL_PASSWORD"),
            database=crawler.settings.get("MYSQL_DB"),
        )

    def open_spider(self, spider):
        # 连接数据库
        self.conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset="utf8mb4"
        )
        self.cursor = self.conn.cursor()

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()

    def process_item(self, item, spider):

        data = ItemAdapter(item).asdict()

        # SQL 更新语句
        sql = """
        UPDATE collection_products
        SET images = %s
        WHERE handle = %s
        """

        try:
            # 执行更新
            self.cursor.execute(sql, (
                data.get("images"),  # 要更新的内容
                data.get("handle"),  # 条件字段
            ))
            self.conn.commit()

            spider.logger.info(f'✅ Updated images for handle: {data.get("handle")}')
        except Exception as e:
            spider.logger.error(f"Update error: {e}")
            self.conn.rollback()

        return item


class UpdateTaskTableProductNumber:

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host=crawler.settings.get("MYSQL_HOST"),
            port=crawler.settings.getint("MYSQL_PORT"),
            user=crawler.settings.get("MYSQL_USER"),
            password=crawler.settings.get("MYSQL_PASSWORD"),
            database=crawler.settings.get("MYSQL_DB"),
        )

    def process_item(self, item, spider):
        return item

    def close_spider(self, spider):
        conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset="utf8mb4"
        )
        cursor = conn.cursor()
        cursor.execute("""UPDATE collection_bath_tasks
                                SET 
                                  quantity = (SELECT COUNT(*) FROM collection_products WHERE task_id = %s),
                                  lowest_price = (SELECT MIN(original_price) FROM collection_products WHERE task_id = %s),
                                  highest_price = (SELECT MAX(original_price) FROM collection_products WHERE task_id = %s)
                                WHERE rid = %s""",
                       (spider.task_id, spider.task_id, spider.task_id, spider.task_id))
        conn.commit()
        cursor.close()
        conn.close()