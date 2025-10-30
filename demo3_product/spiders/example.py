import scrapy
from scrapy import Request


class ExampleSpider(scrapy.Spider):
    name = "example"
    start_urls = ["https://example.com"]
    allowed_domains = ["example.com"]

    def start_requests(self):

        for i in range(10):

            yield scrapy.Request(
                url=self.start_urls[0],
                callback=self.parse,
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        yield {
            "link": "baidu.com",
            "tags": ["1", "2", "3", "4"],
            "referer": "1,2,3",
            "status": 1
        }
