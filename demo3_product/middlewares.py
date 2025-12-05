# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import random
from scrapy import signals

import requests
from scrapy import signals
from scrapy.http import HtmlResponse
from scrapy.exceptions import IgnoreRequest
import logging

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


logger = logging.getLogger(__name__)


class Demo3ProductSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    async def process_start(self, start):
        # Called with an async iterator over the spider start() method or the
        # maching method of an earlier spider middleware.
        async for item_or_request in start:
            yield item_or_request

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class Demo3ProductDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)

class RandomUserAgentMiddleware:
    def __init__(self, user_agents):
        self.user_agents = user_agents

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get('USER_AGENTS_LIST'))

    def process_request(self, request, spider):
        request.headers['User-Agent'] = random.choice(self.user_agents)





class DecompressionMiddleware:
    """
    正确处理各种压缩格式的下载器中间件
    """

    def __init__(self, settings):
        self.settings = settings
        self.session = requests.Session()

        # 配置session，明确指定支持的编码
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'Accept-Encoding': 'gzip, deflate'  # 只支持我们知道如何处理的编码
        })

    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(crawler.settings)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def spider_opened(self, spider):
        spider.logger.info("DecompressionMiddleware opened")

    def spider_closed(self, spider):
        self.session.close()

    def process_request(self, request, spider):
        """
        处理请求，手动处理压缩
        """
        try:
            # 准备requests参数，禁用自动解压
            kwargs = {
                'headers': self._normalize_headers(request.headers),
                'cookies': dict(request.cookies),
                'allow_redirects': not request.meta.get('dont_redirect', False),
                'timeout': request.meta.get('download_timeout', 30),
                'verify': not request.meta.get('dont_verify_ssl', False),
                'stream': True  # 使用流式响应
            }

            # 移除Accept-Encoding头，避免服务器返回我们不支持的编码
            kwargs['headers'].pop('Accept-Encoding', None)
            kwargs['headers'].pop('accept-encoding', None)

            # 处理请求体
            if request.body:
                kwargs['data'] = request.body

            # 处理代理
            if 'proxy' in request.meta:
                kwargs['proxies'] = {'http': request.meta['proxy'], 'https': request.meta['proxy']}

            # 发送请求
            response = self.session.request(
                method=request.method,
                url=request.url,
                **kwargs
            )

            # 手动处理压缩
            return self._handle_compressed_response(response, request)

        except Exception as e:
            logger.error(f"Download failed for {request.url}: {str(e)}")
            return None

    def _normalize_headers(self, scrapy_headers):
        """标准化headers格式"""
        normalized = {}

        for key, values in scrapy_headers.items():
            # 处理key
            key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)

            # 处理values - 只取第一个值
            if isinstance(values, (list, tuple)) and values:
                value_str = values[0]
            else:
                value_str = values or b''

            # 转换为字符串
            if isinstance(value_str, bytes):
                value_str = value_str.decode('utf-8', errors='ignore')

            normalized[key_str] = value_str

        return normalized

    def _handle_compressed_response(self, response, request):
        """手动处理压缩响应"""
        content_encoding = response.headers.get('Content-Encoding', '').lower()
        content = response.content

        # 根据Content-Encoding手动解压
        if content_encoding:
            try:
                if content_encoding == 'gzip':
                    content = self._decompress_gzip(content)
                elif content_encoding == 'deflate':
                    content = self._decompress_deflate(content)
                elif content_encoding == 'br':
                    content = self._decompress_brotli(content)
                else:
                    logger.warning(f"Unsupported content encoding: {content_encoding}")
                    # 尝试自动检测压缩格式
                    content = self._try_decompress(content)
            except Exception as e:
                logger.error(f"Decompression failed for {request.url}: {str(e)}")
                # 如果解压失败，返回原始内容

        # 创建响应对象
        headers = dict(response.headers)
        # 移除content-encoding头，因为我们已经解压了
        if 'content-encoding' in headers:
            del headers['content-encoding']
        if 'content-length' in headers:
            del headers['content-length']

        # 设置正确的内容长度
        headers['Content-Length'] = str(len(content))

        return HtmlResponse(
            url=response.url,
            status=response.status_code,
            headers=headers,
            body=content,
            request=request,
            encoding=response.encoding or 'utf-8'
        )

    def _decompress_gzip(self, data):
        """解压gzip数据"""
        return gzip.decompress(data)

    def _decompress_deflate(self, data):
        """解压deflate数据"""
        try:
            # 尝试zlib解压
            return zlib.decompress(data)
        except zlib.error:
            # 如果失败，尝试raw deflate
            return zlib.decompress(data, -15)

    def _decompress_brotli(self, data):
        """解压brotli数据"""
        try:
            import brotli
            return brotli.decompress(data)
        except ImportError:
            logger.warning("Brotli compression detected but brotli library not installed")
            # 如果没有brotli库，尝试其他方法或者返回原始数据
            return data
        except Exception as e:
            logger.error(f"Brotli decompression failed: {str(e)}")
            return data

    def _try_decompress(self, data):
        """尝试自动检测并解压数据"""
        # 检查是否是gzip格式 (前两个字节是 0x1f 0x8b)
        if len(data) >= 2 and data[:2] == b'\x1f\x8b':
            try:
                return self._decompress_gzip(data)
            except:
                pass

        # 检查是否是zlib/deflate格式
        if len(data) >= 2:
            try:
                return self._decompress_deflate(data)
            except:
                pass

        # 如果都不是，返回原始数据
        return data