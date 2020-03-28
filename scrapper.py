from scrapy import Request, Spider
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from pathlib import Path
import json

current_directory = Path(__file__).parent.absolute()
regions_file = current_directory/'data'/'regions.json'
provinces_file = current_directory/'data'/'provinces.json'
citimuni_file = current_directory/'data'/'citimuni.json'
barangays_file = current_directory/'data'/'barangays.json'


class RegionSpider(Spider):
    name = "regions"
    base_url = 'https://psa.gov.ph/classification/psgc'

    custom_settings = {
        'FEED_URI': regions_file.as_uri(),
        'FEED_FORMAT': 'json'
    }

    def start_requests(self):
        urls = [
            f'{self.base_url}/?q=psgc/regions',
        ]
        for url in urls:
            yield Request(url=url, callback=self.parse)

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
                    provinces=f'{self.base_url}/?q=psgc/provinces/{code}',
                    citimuni=f'{self.base_url}/?q=psgc/citimuni/{code}',
                    barangays=f'{self.base_url}/?q=psgc/barangays/{code}'
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


class ProvinceSpider(Spider):
    name = "provinces"
    base_url = 'https://psa.gov.ph/classification/psgc'

    custom_settings = {
        'FEED_URI': provinces_file.as_uri(),
        'FEED_FORMAT': 'json'
    }

    def start_requests(self):
        regions = []
        with open(regions_file, 'r') as file:
            regions = json.load(file)

        for region in regions:
            yield Request(
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
                    provinces=f'{self.base_url}/?q=psgc/provinces/{code}',
                    citimuni=f'{self.base_url}/?q=psgc/citimuni/{code}',
                    barangays=f'{self.base_url}/?q=psgc/barangays/{code}'
                ),
                info=info,
                income_class=income_class,
                stats=dict(
                    population=population
                )
            )

            yield(province)


class CitiMuniSpider(Spider):
    name = "citimuni"
    base_url = 'https://psa.gov.ph/classification/psgc'

    custom_settings = {
        'FEED_URI': citimuni_file.as_uri(),
        'FEED_FORMAT': 'json'
    }

    def start_requests(self):
        provinces = []
        with open(provinces_file, 'r') as file:
            provinces = json.load(file)

        for province in provinces:
            yield Request(
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
                    provinces=f'{self.base_url}/?q=psgc/provinces/{code}',
                    citimuni=f'{self.base_url}/?q=psgc/citimuni/{code}',
                    barangays=f'{self.base_url}/?q=psgc/barangays/{code}'
                ),
                income_class=income_class,
                stats=dict(
                    population=population
                )
            )

            yield(citimuni)


class BarangaySpider(Spider):
    name = "barangays"
    base_url = 'https://psa.gov.ph/classification/psgc'

    custom_settings = {
        'FEED_URI': barangays_file.as_uri(),
        'FEED_FORMAT': 'json'
    }

    def start_requests(self):
        citimuni = []
        with open(citimuni_file, 'r') as file:
            citimuni = json.load(file)

        for _citimuni in citimuni:
            yield Request(
                url=_citimuni['url']['barangays'],
                callback=self.parse,
                cb_kwargs=dict(
                    region_code=_citimuni['region_code'],
                    province_code=_citimuni['province_code'],
                    citimuni_code=_citimuni['code']
                )
            )

    def parse(self, response, region_code, province_code, citimuni_code):
        tables = response.css('table#classifytable')
        try:
            x, barangay_table = tables
        except e:
            input()
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
                    provinces=f'{self.base_url}/?q=psgc/provinces/{code}',
                    citimuni=f'{self.base_url}/?q=psgc/citimuni/{code}',
                    barangays=f'{self.base_url}/?q=psgc/barangays/{code}'    
                ),
                type=_type,
                stats=dict(
                    population=population
                )
            )

            yield(barangay)

        next_page = response.css('li.pager-next a::attr(href)').get()
        if next_page:
            url = f'${self.base_url}/${next_page}'
            yield Request(
                url=url,
                callback=self.parse,
                cb_kwargs=dict(
                    region_code=region_code,
                    province_code=province_code,
                    citimuni_code=citimuni_code
                )
            )


configure_logging()
runner = CrawlerRunner()

@defer.inlineCallbacks
def crawl():
    yield runner.crawl(RegionSpider)
    yield runner.crawl(ProvinceSpider)
    yield runner.crawl(CitiMuniSpider)
    yield runner.crawl(BarangaySpider)
    reactor.stop()

crawl()
reactor.run()
