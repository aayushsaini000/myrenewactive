import re
import csv
import json
import time
import scrapy
import requests
from lxml.html import fromstring
from scrapy.crawler import CrawlerProcess
import csv
from uszipcode import SearchEngine

# PROXY = '104.43.230.151:3128'
PROXY = "45.79.220.141:3128"


def get_proxies_from_free_proxy():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.content)
    proxies = set()
    for i in parser.xpath('//tbody/tr'):
        if i.xpath('.//td[3][text()="US"]') and\
           i.xpath('.//td[7][contains(text(),"yes")]'):
            ip = i.xpath('.//td[1]/text()')[0]
            port = i.xpath('.//td[2]/text()')[0]
            proxies.add("{}:{}".format(ip, port))
            if len(proxies) == 20:
                return proxies
    return proxies


def get_states():
    return [
        "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
        "Connecticut", "Delaware", "District of Columbia", "Florida",
        "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas",
        "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts",
        "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana",
        "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico",
        "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma",
        "Oregon", "Pennsylvania", "Puerto Rico", "Rhode Island",
        "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
        "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin",
        "Wyoming"
    ]


def get_zip_codes_map():
    search = SearchEngine()
    zipcodes = list()
    for state in get_states():
    # for state in ['New York']:
        final_response = list()
        response = search.by_state(state, returns=2000)
        for r in response:
            if r.major_city not in [x.major_city for x in final_response]:
                final_response.append(r)
        for res in response:
            if res:
                zipcodes.append({
                    'zip_code': res.zipcode,
                    'latitude': res.lat,
                    'longitude': res.lng,
                    'city': res.major_city,
                    'state': res.state
                })
    return sorted(zipcodes, key=lambda k: k['state'])


def get_token():
    token_url = 'https://www.myrenewactive.com/content/fit/'\
                'member/en/secure/jcr:content.fittoken.json'
    params = {
        "timestamp": int(time.time()*1000)
    }
    response = requests.get(token_url, params=params)
    if not response.status_code == 200:
        return
    return response.json()['accesstoken']


def get_headers():
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
        "actor": "default",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": "www.myrenewactive.com",
        "ismock": "false",
        "Pragma": "no-cache",
        "Referer": "https://www.myrenewactive.com/content/fit/member"
                   "/en/secure/home.html",
        "scope": "read",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"
                      "71.0.3578.98 Safari/537.36",
    }
    # token = get_token()
    # headers.update({
    #     "Authorization": f"Bearer {token}"
    # })
    return headers


class ExtractItem(scrapy.Item):
    # define the fields for your item here like:
    gymId = scrapy.Field()
    location = scrapy.Field()
    address = scrapy.Field()
    city = scrapy.Field()
    state = scrapy.Field()
    zipcode = scrapy.Field()
    phoneNumber = scrapy.Field()
    gymType = scrapy.Field()
    website = scrapy.Field()
    pool = scrapy.Field()
    groupFitness = scrapy.Field()
    onlineEnrollment = scrapy.Field()
    notes = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()
    domainId = scrapy.Field()
    cardio = scrapy.Field()
    strength = scrapy.Field()
    aquatic = scrapy.Field()
    specialty = scrapy.Field()
    personalFitnessPlan = scrapy.Field()
    mindAndBody = scrapy.Field()
    distance = scrapy.Field()
    solrLocation = scrapy.Field()
    pass


class RenewActiveSpider(scrapy.Spider):
    name = "renew_active_spider"
    allowed_domains = ["myrenewactive.com"]
    scraped_data = list()
    fieldnames = [
        "gymId", "location", "address", "city", "state", "zipcode",
        "phoneNumber", "gymType", "website", "pool", "groupFitness",
        "onlineEnrollment", "notes", "latitude", "longitude",
        "domainId", "cardio", "strength", "aquatic", "specialty",
        "personalFitnessPlan", "mindAndBody", "distance", "solrLocation"
    ]
    
    def start_requests(self):
        base_url = 'https://uhcrenewactive.com/play/rest/incentives/v2/geolocation/fitnessLocations/v1/gyms?distance=50'
        headers = get_headers()
        zip_codes_map = get_zip_codes_map()
        for index, zip_code_map in enumerate(zip_codes_map, 1):
            url = "&zipCode={zip_code_map['zip_code']}"
            url ="{0}&zipCode={1}".format(base_url, zip_code_map['zip_code'])
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                # headers=headers,
                dont_filter=True
            )

    def parse(self, response):
        if not response.status == 200:
            return
        results = json.loads(response.text)
        for result in results:
            if result.get('gymId') not in self.scraped_data:
                item = ExtractItem()
                dict_to_write = {k: result[k] for k in self.fieldnames}
                item.update(dict_to_write)
                self.scraped_data.append(result['gymId'])
                yield item



def run_spider(no_of_threads, request_delay):
    settings = {
        'ITEM_PIPELINES': {
            'pipelines.ExtractPipeline': 300,
        },
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
            'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
        },
        'DOWNLOAD_DELAY': request_delay,
        'CONCURRENT_REQUESTS': no_of_threads,
        'CONCURRENT_REQUESTS_PER_DOMAIN': no_of_threads,
        'RETRY_HTTP_CODES': [403, 429, 500, 503],
        'ROTATING_PROXY_LIST': PROXY,
        'ROTATING_PROXY_BAN_POLICY': 'pipelines.BanPolicy',
        'RETRY_TIMES': 10,
        'LOG_ENABLED': True,

    }
    process = CrawlerProcess(settings)
    process.crawl(RenewActiveSpider)
    process.start()

if __name__ == '__main__':
    no_of_threads = 40
    request_delay = 0.01
    run_spider(no_of_threads, request_delay)
