import scrapy
from scrapy.crawler import CrawlerProcess
from pathlib import Path
import json

current_directory = Path(__file__).parent.absolute()
regions_file = current_directory/'data'/'regions.json'
provinces_file = current_directory/'data'/'provinces.json'
citimuni_file = current_directory/'data'/'citimuni.json'


class RegionSpider(scrapy.Spider):
    name = "regions"
    base_url = 'https://psa.gov.ph/classification/psgc/?q=psgc'

    custom_settings = {
        'FEED_URI': regions_file.as_uri(),
        'FEED_FORMAT': 'json'
    }

    def start_requests(self):
        urls = [
            f'{self.base_url}/regions',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
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
                    provinces=f'{self.base_url}/provinces/{code}',
                    citimuni=f'{self.base_url}/citimuni/{code}',
                    barangays=f'{self.base_url}/barangays/{code}'
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
    base_url = 'https://psa.gov.ph/classification/psgc/?q=psgc'

    custom_settings = {
        'FEED_URI': provinces_file.as_uri(),
        'FEED_FORMAT': 'json'
    }

    def start_requests(self):
        regions = []
        with open(regions_file, 'r') as file:
            regions = json.load(file)

        for region in regions:
            yield scrapy.Request(
                url=region['urls']['provinces'],
                callback=self.parse,
                cb_kwargs=dict(region_code=region['code'])
            )

    def parse(self, response, region_code):
        tables = response.css('table#classifytable')
        x, province_table = tables
        province_rows = province_table.css('tbody > tr')

        for row in province_rows:
            row_text = [x.get() for x in row.css('td > a::text')]\
                     + [x.get() for x in row.css('td::text')]
            income_class = ''
            if len(row_text) < 5:
                name, code, info, population = row_text
            else:
                name, code, info, income_class, population = row_text

            province = dict(
                code=code,
                name=name,
                region_code=region_code,
                url=dict(
                    provinces=f'{self.base_url}/provinces/{code}',
                    citimuni=f'{self.base_url}/citimuni/{code}',
                    barangays=f'{self.base_url}/citimuni/{code}'
                ),
                info=info,
                income_class=income_class,
                stats=dict(
                    population=population
                )
            )

            yield(province)


class ProvinceSpider(scrapy.Spider):
    name = "provinces"
    base_url = 'https://psa.gov.ph/classification/psgc/?q=psgc'

    custom_settings = {
        'FEED_URI': citimuni_file.as_uri(),
        'FEED_FORMAT': 'json'
    }

    def start_requests(self):
        provinces = []
        with open(provinces_file, 'r') as file:
            provinces = json.load(file)

        for province in provinces:
            yield scrapy.Request(
                url=province['urls']['citimuni'],
                callback=self.parse,
                cb_kwargs=dict(
                    region_code=province['region_code'],
                    province_code=province['code']
                )
            )

    def parse(self, response, region_code):
        pass


regionProcess.crawl(RegionSpider)
regionProcess.start()

provinceProcess.crawl(ProvinceSpider)
provinceProcess.start()
