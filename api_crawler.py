from txrequests import Session
from typing import Callable, Optional
from http.cookiejar import CookieJar
from threading import Lock
from lxml import html
from string import Template
import requests
import json
import logging
import datetime
import datetimerange
import time
import os


class Website:
    def __init__(self, website: str, api_path: str = "api/timeline.json", language: str = "www", method: str = "GET",
                 default_query_params: dict = None, default_data: dict = None, url_format="https://$language.$website"):
        self.url = Template(url_format).substitute(language=language, website=website)
        self.api_path = api_path
        self.language = language
        self.method = method
        self.api_url = f"{self.url}/{api_path}"
        self.queried_timeranges = []  # the datetime ranges where articles where searched for
        self.default_query_params = default_query_params
        if default_query_params is None:
            self.default_query_params = dict()
        self.default_data = default_data
        if default_data is None:
            self.default_data = dict()
        self.lock = Lock()
        self.sleep_time = 1  # the incrementing time to pause requests to this website after a bad response code

    def update_queried_timestamps(self, newly_queried_timerange: datetimerange.DateTimeRange):
        """
        Updates the time ranges of this website in which articles were already crawled.
        :param newly_queried_timerange: the time range of the just queried articles
        """
        with self.lock:  # synchronize because this methods gets called from a background thread
            self.queried_timeranges.append(newly_queried_timerange)
            # we do not need to union time ranges if the lists size is smaller than 1
            if len(self.queried_timeranges) <= 1:
                return
            self.queried_timeranges.sort(key=lambda x: x.start_datetime)
            current_timerange_index = 0
            # unite time ranges inplace
            while current_timerange_index < len(self.queried_timeranges) - 1:
                time_range = self.queried_timeranges[current_timerange_index + 1]
                if time_range.is_intersection(self.queried_timeranges[current_timerange_index]):
                    # if the two time ranges intersect, build the union and remove old time ranges from the list
                    current_time_range = self.queried_timeranges.pop(current_timerange_index)
                    self.queried_timeranges.pop(current_timerange_index)
                    union_range = current_time_range.encompass(time_range)
                    # insert the union back into the list
                    self.queried_timeranges.insert(current_timerange_index, union_range)
                else:
                    current_timerange_index += 1

    def get_surrounding_timerange(self, time: datetime.datetime) -> Optional[datetimerange.DateTimeRange]:
        timerange: datetimerange.DateTimeRange
        for timerange in self.queried_timeranges:
            if timerange.__contains__(time):
                return timerange
        return None

    def __str__(self):
        return f"{self.method} {self.api_url}"


class Crawler:
    def __init__(self, max_concurrent_requests):
        self.session = Session(maxthreads=max_concurrent_requests)

    def add_website_request(self, website: Website, callback: Callable[[Session, requests.Response], requests.Response],
                            query_params: dict, data: dict):
        self.add_request(website.method, website.api_url, callback, query_params, data)

    def add_request(self, method: str, url: str, callback: Callable[[Session, requests.Response], requests.Response],
                    query_params: dict, data: dict):
        logging.debug(f"{method} {url} [params: {query_params}, data: {data}]")
        self.session.request(method=method, url=url, params=query_params, #data=data,
                             background_callback=callback)

    def add_cookies(self, cookies: CookieJar):
        self.session.cookies.update(cookies)

    def stop(self):
        self.session.close()


class EuroNewsCrawler(Crawler):
    def __init__(self, database, max_database_size: int, max_requests, working_dir):
        super().__init__(max_requests)
        self.response_handlers = []
        assert os.path.isdir(working_dir), "path is not a directory"
        assert os.access(working_dir, os.W_OK), "directory not writeable"
        self.working_dir = working_dir
        self.max_database_size = max_database_size
        self.websites = [
            # limit describes the number of articles fetched per request
            Website("euronews.com", default_query_params={"limit": 50}), #check
            Website("euronews.com", language="de", default_query_params={"limit": 50}), #check
            Website("euronews.com", language="fr", default_query_params={"limit": 50}), #check
            Website("euronews.com", language="it", default_query_params={"limit": 50}), #check
            Website("euronews.com", language="es", default_query_params={"limit": 50}), #check
            Website("euronews.com", language="pt", default_query_params={"limit": 50}), #check
            Website("euronews.com", language="ru", default_query_params={"limit": 50}), #check
            Website("euronews.com", language="tr", default_query_params={"limit": 50}), #check?
            Website("euronews.com", language="gr", default_query_params={"limit": 50}), #check
            Website("euronews.com", language="hu", default_query_params={"limit": 50}), #check
            Website("euronews.com", language="per", default_query_params={"limit": 50}), #check
            Website("euronews.com", language="arabic", default_query_params={"limit": 50}), #check
        ]
        self.db = database
        self.load_progress()

    def start(self, start_crawling_dates=None):
        if start_crawling_dates is None:
            start_crawling_dates = []
        logging.info("Starting crawler for librivox api for new articles...")
        article_count = self.db.get_not_downloaded_article_count()
        if article_count >= self.max_database_size:
            logging.info(f"No fetching of new articles because the database is still big enough")
            return
        for website in self.websites:
            for start_date in start_crawling_dates:
                website.update_queried_timestamps(datetimerange.DateTimeRange(start_date, start_date))
            # Start by scheduling a default request to the api of each website
            self.create_website_request(website)

    def register_response_handler(self, handler: Callable[[Website, dict], None]):
        self.response_handlers.append(handler)

    def persist_progress(self):
        logging.debug("Persisting progress of websites..")
        for website in self.websites:
            self.db.store_website(website)

    def load_progress(self):
        for website in self.websites:
            website.queried_timeranges = self.db.load_website(website.language)

    def continue_crawling(self):
        for website in self.websites:
            self.continue_website_crawling(website)

    def continue_website_crawling(self, website: Website):
        for time_range in website.queried_timeranges:
            end_time = time_range.start_datetime  # use the start time because we search from the present into the past
            self.continue_website_crawling_after_time(website, end_time)

    def continue_website_crawling_after_time(self, website: Website, date_upper_limit: datetime.datetime = None):
        if date_upper_limit is None:
            date_upper_limit = datetime.datetime.now().replace(microsecond=0)
        after = int(date_upper_limit.replace(tzinfo=datetime.timezone.utc).timestamp())
        params = {"after": after}
        logging.info(f"[{website.language}] Continue searching articles older than {date_upper_limit}")
        self.add_website_request(website, query_params=params, callback=self.process_response, data=website.default_data)

    def create_website_request(self, website: Website):
        # start a request for the newest articles
        now = datetime.datetime.now().replace(microsecond=0)
        website.update_queried_timestamps(datetimerange.DateTimeRange(now, now))
        self.continue_website_crawling_after_time(website, now)

    def add_website_request(self, website: Website, callback: Callable[[dict, requests.Response], requests.Response],
                            query_params: dict = None, data: dict = None):
        if query_params is None:
            query_params = {}
        default_query_params = website.default_query_params.copy()
        default_query_params.update(query_params)  # overwrite default headers with given ones
        super().add_website_request(website, lambda session, response: callback(default_query_params, response),
                                    default_query_params, data)

    def process_response(self, query_params: dict, response: requests.Response) -> requests.Response:
        website: Optional[Website] = self.get_website(response.request)
        max_time = datetime.datetime.utcfromtimestamp(query_params["after"])
        # handle error cases
        if website is None:
            logging.error(f"Could not find website object for response from {response.url}")
            return response
        if response.status_code != 200:
            logging.info(f"Received {response.status_code} from {response.url} - repeating request")
            time.sleep(website.sleep_time)
            website.sleep_time *= 2
            self.continue_website_crawling_after_time(website, max_time)
            return response
        else:
            website.sleep_time = 1
        content = json.loads(response.content)
        if len(content) > 0:
            logging.debug(f"Loaded {len(content)} articles from {website.api_url}")
            time_property_name = "publishedAt"
            min_time = datetime.datetime.utcfromtimestamp(content[0][time_property_name])
            for entry in content:
                # notify handlers that a new article was found
                for response_handler in self.response_handlers:
                    response_handler(website, entry)
                last_updated = datetime.datetime.utcfromtimestamp(entry[time_property_name])
                if last_updated < min_time:
                    min_time = last_updated
            website.update_queried_timestamps(datetimerange.DateTimeRange(min_time, max_time))
            surrounding_timerange = website.get_surrounding_timerange(min_time)
            article_count = self.db.get_not_downloaded_article_count()
            if surrounding_timerange is not None:
                if article_count < self.max_database_size:
                    self.continue_website_crawling_after_time(website, surrounding_timerange.start_datetime)
                else:
                    logging.debug(f"Stop crawling api {website.api_url} for now because database size limit reached")
            else:
                logging.error(f"Could not find next timestamp to query for ({website})")
        else:
            logging.info(f"finished crawling {website.api_url}, waiting for refresh interval now")
        return response

    def get_website(self, request: requests.PreparedRequest) -> Optional[Website]:
        for website in self.websites:
            if request.url.startswith(website.api_url):
                return website
        return None