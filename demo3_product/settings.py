# Scrapy settings for demo3_product project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "demo3_product"

SPIDER_MODULES = ["demo3_product.spiders"]
NEWSPIDER_MODULE = "demo3_product.spiders"

ADDONS = {}


# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Concurrency and throttling settings
#CONCURRENT_REQUESTS = 16
CONCURRENT_REQUESTS_PER_DOMAIN = 4
DOWNLOAD_DELAY = 1

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "demo3_product.middlewares.Demo3ProductSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   # "demo3_product.middlewares.Demo3ProductDownloaderMiddleware": 543,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    # "demo3_product.pipelines.Demo3ProductPipeline": 300,
    # "demo3_product.pipelines.UpdateImagesPipline": 295,
    "demo3_product.pipelines.MySQLPipeline": 295,
    "demo3_product.pipelines.ElasticsearchPipeline": 300,
    "demo3_product.pipelines.UpdateTaskTableProductNumber": 305,
    # "demo3_product.pipelines.PostgresUpdatePipeline": 305,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
FEED_EXPORT_ENCODING = "utf-8"

LOG_LEVEL = 'INFO'


MYSQL_HOST = '107.150.40.2'
MYSQL_PORT = 3306
MYSQL_USER = 'data_softwared'
MYSQL_DB = 'data_softwared'
MYSQL_PASSWORD = 'QGX6keKzm5P16xQ6'


ES_HOST = "https://107.150.40.2:9200"
ES_USER = "elastic"
ES_PASS = "dnabYdQtr9s_rvgH6dGD"
INDEX_NAME = "product_index"

POSTGRES_HOST = '107.150.40.2'
POSTGRES_PASSWORD = 'S4ssbeXn6zeDs8ij'
POSTGRES_PORT = 5432
POSTGRES_USER = 'postgres'
POSTGRES_DBNAME = 'postgres'


# redis
REDIS_ENCODING = None
REDIS_DECODE_RESPONSES = False


# 使用 scrapy-redis 的调度器
SCHEDULER = "scrapy_redis.scheduler.Scheduler"

# 让所有爬虫共享一个去重容器
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"

# 是否在关闭时保留队列（断点续爬）
SCHEDULER_PERSIST = True

# Redis 连接信息
REDIS_HOST = '107.150.40.2'
REDIS_PORT = 6379
REDIS_PARAMS = {
    'password': 'pFKfclD2rU$3lib@6',
    'decode_responses': False,
}

# 可选：防止爬虫重复抓取时清空队列
SCHEDULER_FLUSH_ON_START = False


# USER_AGENTS_LIST = [
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0',
# ]

