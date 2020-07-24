import os
import logging
from typing import Optional, Tuple

from tinydb import TinyDB, Query
from datetimerange import DateTimeRange
from api_crawler import Website
from threading import Lock


class Database:
    website_type = "website"
    article_type = "article"

    def __init__(self, working_dir: str):
        assert os.path.isdir(working_dir), "no working directory given"
        self.storage_file = os.path.join(working_dir, "db.json")
        self.db = TinyDB(self.storage_file)
        self.lock = Lock()

    def store_website(self, website: Website):
        with self.lock:
            website_query = self.create_website_query(website.language)
            obj = self.create_website_object(website)
            obj["time_ranges"] = self.convert_from_datetime_range(website.queried_timeranges)
            if any(self.db.search(website_query)):
                self.db.update(obj, website_query)
            else:
                self.db.insert(obj)

    def load_website(self, language: str) -> list[DateTimeRange]:
        with self.lock:
            website_query = self.create_website_query(language)
            found_objects = self.db.search(website_query)
            if len(found_objects) > 1:
                logging.error(f"language {language} has multiple websites stored in db")
            if any(found_objects):
                return self.convert_to_datetime_range(found_objects[0]["time_ranges"])
            else:
                return []

    def store_article(self, article_id: str, language: str, full_url: str, article_dir: str):
        with self.lock:
            article_query = self.create_article_query(article_id, language)
            if any(self.db.search(article_query)):  # skip this article if we already found it in the past
                return
            obj = self.create_article_object(article_id, language)
            obj["full_url"] = full_url
            obj["crawl_status"] = 0
            obj["article_dir"] = article_dir
            self.db.insert(obj)

    def get_article_to_crawl(self) -> Optional[Tuple[str, str]]:
        """
        Returns a tuple with the url and the storage dir of an article to crawl if possible, else None.
        As a sideeffect, it updates the status for this article so it does not get crawled again
        :return: a tuple of url and file disk dir or None if no article could be found
        """
        with self.lock:
            article_query = Query()
            found_articles = self.db.search(
                (article_query.type == self.article_type) & (article_query.crawl_status == 0))
            if len(found_articles) > 0:
                article = found_articles[0]
                article_query = self.create_article_query(article["id"], article["language"])
                self.db.update({"crawl_status": 1}, article_query)
                return article["full_url"], article["article_dir"]
            return None

    def set_stored_article_to_crawled(self, article_id: str, language: str):
        with self.lock:
            article_query = self.create_article_query(article_id, language)
            found_objects = self.db.search(article_query)
            if len(found_objects) > 1:
                logging.error(f"language {language} has multiple articles with id {article_id} stored in db")
            if any(found_objects):
                self.db.update({"crawl_status": 2}, article_query)

    def create_article_object(self, article_id: str, language: str) -> dict:
        return {
            "type": self.article_type,
            "id": article_id,
            "language": language
        }

    def create_article_query(self, article_id: str, language: str) -> Query:
        query = Query()
        query = (query.type == self.article_type) & (query.article_id == article_id) & (query.language == language)
        return query

    def create_website_object(self, website: Website):
        return {
            "type": self.website_type,
            "language": website.language
        }

    def create_website_query(self, language: str):
        query = Query()
        query = (query.type == self.website_type) & (query.language == language)
        return query

    @staticmethod
    def convert_from_datetime_range(time_ranges):
        result = []
        for time_range in time_ranges:
            if isinstance(time_range, DateTimeRange):
                result.append({"start": time_range.get_start_time_str(), "end": time_range.get_end_time_str()})
        return result

    @staticmethod
    def convert_to_datetime_range(time_ranges: list) -> list:
        result = []
        for start_end_dict in time_ranges:
            result.append(DateTimeRange(start_end_dict["start"], start_end_dict["end"]))
        return result
