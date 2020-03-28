import scrapy
from scrapy.crawler import CrawlerProcess
from pathlib import Path
import json

current_directory = Path(__file__).parent.absolute()
regions_file = current_directory/'data'/'regions.json'
provinces_file = current_directory/'data'/'provinces.json'
citimuni_file = current_directory/'data'/'citimuni.json'
barangays_file = current_directory/'data'/'barangays.json'


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
                url=dict(
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
                url=region['url']['provinces'],
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


class CitiMuniSpider(scrapy.Spider):
    name = "citimuni"
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
                url=province['url']['citimuni'],
                callback=self.parse,
                cb_kwargs=dict(
                    region_code=province['region_code'],
                    province_code=province['code']
                )
            )

    def parse(self, response, region_code, province_code):
        tables = response.css('table#classifytable')
        x, *citimuni_tables = tables

        citimuni_rows = []
        for citimuni_table in citimuni_tables:
            citimuni_rows = citimuni_rows + citimuni_table.css('tbody > tr')

        for row in citimuni_rows:
            row_text = [x.get() for x in row.css('td > a::text')]\
                     + [x.get() for x in row.css('td::text')]
            
            name, code, income_class, population = row_text

            citimuni = dict(
                code=code,
                name=name,
                region_code=region_code,
                province_code=province_code,
                url=dict(
                    provinces=f'{self.base_url}/provinces/{code}',
                    citimuni=f'{self.base_url}/citimuni/{code}',
                    barangays=f'{self.base_url}/citimuni/{code}'
                ),
                income_class=income_class,
                stats=dict(
                    population=population
                )
            )

            yield(citimuni)


class BarangaySpider(scrapy.Spider):
    name = "barangays"
    base_url = 'https://psa.gov.ph/classification/psgc/?q=psgc'

    custom_settings = {
        'FEED_URI': barangays_file.as_uri(),
        'FEED_FORMAT': 'json'
    }

    def start_requests(self):
        citimuni = []
        with open(citimuni_file, 'r') as file:
            citimuni = json.load(file)

        for _citimuni in citimuni:
            yield scrapy.Request(
                url=_citimuni['url']['citimuni'],
                callback=self.parse,
                cb_kwargs=dict(
                    region_code=_citimuni['region_code'],
                    province_code=_citimuni['province_code'],
                    citimuni_code=_citimuni['code']
                )
            )

    def parse(self, response, region_code, province_code, citimuni_code):
        tables = response.css('table#classifytable')
        x, barangay_table = tables
        barangay_rows = barangay_table.css('tbody > tr')

        for row in barangay_rows:
            row_text = [x.get() for x in row.css('td > a::text')]\
                     + [x.get() for x in row.css('td::text')]   
            name, code, _type, population = row_text

            barangay = dict(
                code=code,
                name=name,
                region_code=region_code,
                province_code=province_code,
                citimuni_code=citimuni_code,
                url=dict(
                    provinces=f'{self.base_url}/provinces/{code}',
                    citimuni=f'{self.base_url}/citimuni/{code}',
                    barangays=f'{self.base_url}/citimuni/{code}'    
                ),
                type=_type,
                stats=dict(
                    population=population
                )
            )

            yield(barangay)

        next_page = response.css('li.pager-next a::attr(href)').get()
        if next_page:
            yield scrapy.Request(
                url=next_page,
                callback=self.parse,
                cb_kwargs=dict(
                    region_code=region_code,
                    province_code=province_code,
                    citimuni_code=citimuni_code
                )
            )



regionProcess.crawl(RegionSpider)
regionProcess.start()

provinceProcess.crawl(ProvinceSpider)
provinceProcess.start()

citiMuniProcess.crawl(CitiMuniSpider)

citiMuniProcess.start()
