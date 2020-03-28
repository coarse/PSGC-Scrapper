import scrapy
from scrapy.crawler import CrawlerProcess
from pathlib import Path

current_directory = Path(__file__).parent.absolute()
regions_file = current_directory/'data'/'regions.json'
provinces_file = current_directory/'data'/'provinces.json'


class RegionSpider(scrapy.Spider):
    name = "regions"

    custom_settings = {
        'FEED_URI': regions_file.as_uri(),
        'FEED_FORMAT': 'json'
    }

    def start_requests(self):
        urls = [
        'https://psa.gov.ph/classification/psgc/?q=psgc/regions',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        base_url = response.url.replace('/regions', '')
        regions = response.css('table#classifytable')

        for region in regions:
            head_text = region.css('thead > tr > th::text')
            name_text, code_text = head_text
            name = name_text.re(r'Region: (.*)').pop()
            code = code_text.re(r'Code: (.*)').pop()

            body_text = region.css('tbody > tr > td::text')
            filtered_body_text = [x.get() for x in body_text if x.get() != 'Population']
            provinces, cities, municipalities, barangays, population = filtered_body_text

            region = dict(
                code=code,
                name=name,
                urls=dict(
                provinces=f'{base_url}/provinces/{code}',
                cities=f'{base_url}/cities/{code}',
                municipalities=f'{base_url}/municipalities/{code}',
                barangays=f'{base_url}/barangays/{code}'
                ),
                stats=dict(
                provinces=provinces,
                cities=cities,
                municipalities=municipalities,
                barangays=barangays,
                population=population
                )
            )

            yield(region)


class ProvinceSpider(scrapy.Spider):
    name = "provinces"

    custom_settings = {
        'FEED_URI': province_file.as_uri(),
        'FEED_FORMAT': 'json'
    }

    def start_requests(self):
        pass

    def parse(self, response):
        pass


regionProcess = CrawlerProcess()

regionProcess.crawl(RegionSpider)
regionProcess.start()