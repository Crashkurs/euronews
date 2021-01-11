import os
import logging
from typing import Optional, Tuple
from tinydb import TinyDB, Query
from tinydb.operations import add, set
from datetimerange import DateTimeRange
from api_crawler import Website
from threading import Lock
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware


class Database:
    website_type = "website"
    article_type = "article"

    def __init__(self, working_dir: str):
        assert os.path.isdir(working_dir), "working directory does not exist or is not valid"
        self.storage_file = os.path.join(working_dir, "db.json")
        self.db = TinyDB(self.storage_file, storage=CachingMiddleware(JSONStorage), indent=4)
        self.lock = Lock()
        self.languages = ["www", "de", "fr", "it", "es", "pt", "ru", "tr", "gr", "hu", "per", "arabic"]

    def store_website(self, website: Website):
        try:
            with self.lock:
                website_query = self.create_website_query(website.language)
                obj = self.create_website_object(website)
                obj["time_ranges"] = self.convert_from_datetime_range(website.queried_timeranges)
                self.get_website_db().upsert(obj, website_query)
        except Exception as e:
            logging.exception(e)

    def load_website(self, language: str) -> list:
        with self.lock:
            website_query = self.create_website_query(language)
            found_objects = self.get_website_db().search(website_query)
            if len(found_objects) > 1:
                logging.error(f"language {language} has multiple websites stored in db")
            if any(found_objects):
                result = self.convert_to_datetime_range(found_objects[0]["time_ranges"])
                logging.info(f"Continue language {language} after {result}")
                return result
            else:
                return []

    def store_article(self, article_id: str, language: str, full_url: str, article_dir: str):
        with self.lock:
            article_query = self.create_article_query(article_id, language)
            articles = self.get_article_db()
            if any(articles.search(article_query)):  # skip this article if we already found it in the past
                return
            obj = self.create_article_object(article_id, language)
            obj["full_url"] = full_url
            obj["crawl_status"] = 0
            obj["article_dir"] = article_dir
            articles.insert(obj)

    def get_article_to_crawl(self) -> Optional[Tuple[str, str, str, str]]:
        """
        Returns a tuple with the id, language, url and storage directory an article to crawl if possible, else None.
        As a sideeffect, it updates the status for this article so it does not get crawled again
        :return: a tuple of (id, language, url, storage_dir) or None if no article could be found
        """
        with self.lock:
            article_query = Query()
            articles = self.get_article_db()
            language = self.languages.pop(0)
            self.languages.append(language)
            found_articles = articles.search(
                (article_query.type == self.article_type) & (article_query.crawl_status == 0)
                & (article_query.language == language))
            logging.info(f"[{language}]Search article to download in db")
            if len(found_articles) > 0:
                article = found_articles[0]
                id = article["id"]
                language = article["language"]
                article_query = self.create_article_query(id, language)
                articles.update(set("crawl_status", 1), article_query)
                return id, language, article["full_url"], article["article_dir"]
            return None, language, None, None

    def increment_crawled_article_status(self, article_id: str, language: str, amount: int = 1):
        with self.lock:
            article_query = Query()
            article_query = (article_query.type == self.article_type) & (article_query.crawl_status >= 1) \
                            & (article_query.id == article_id) & (article_query.language == language)
            articles = self.get_article_db()
            found_objects = articles.search(article_query)
            if len(found_objects) > 1:
                logging.error(f"language {language} has multiple articles with id {article_id} stored in db")
            if any(found_objects):
                articles.update(add("crawl_status", amount), article_query)
        self.delete_downloaded_articles()

    def delete_downloaded_articles(self):
        with self.lock:
            article_query = Query()
            article_query = (article_query.type == self.article_type) & (article_query.crawl_status == 3)
            articles = self.get_article_db()
            articles.remove(article_query)

    def reset_crawled_article_status(self, article_id: str, language: str):
        with self.lock:
            article_query = Query()
            article_query = (article_query.type == self.article_type) & (article_query.crawl_status >= 1) \
                            & (article_query.id == article_id) & (article_query.language == language)
            articles = self.get_article_db()
            found_objects = articles.search(article_query)
            if len(found_objects) > 1:
                logging.error(f"language {language} has multiple articles with id {article_id} stored in db")
            if any(found_objects):
                articles.update(set("crawl_status", 0), article_query)
        self.delete_downloaded_articles()

    def reset_crawled_articles_status(self):
        """
        Resets the 'crawl_status' flag of all articles who were being downloaded before the last shutdown and did not
        finish.
        """
        with self.lock:
            articles = self.get_article_db()
            article_query = Query()
            article_query = (article_query.type == self.article_type) & (article_query.crawl_status >= 1) \
                            & (article_query.crawl_status < 3)
            articles.update(set("crawl_status", 0), article_query)

    def move_article_to_error_list(self, article_id: str, language: str):
        with self.lock:
            article_query = Query()
            article_query = (article_query.type == self.article_type) \
                            & (article_query.id == article_id) & (article_query.language == language)
            articles = self.get_article_db()
            found_objects = articles.search(article_query)
            if len(found_objects) > 1:
                logging.error(f"language {language} has multiple articles with id {article_id} stored in db")
            if any(found_objects):
                articles.remove(article_query)
                for obj in found_objects:
                    self.get_error_db().insert(obj)


    def get_not_downloaded_article_count(self):
        with self.lock:
            article_query = Query()
            article_query = article_query.crawl_status == 0
            articles = self.get_article_db()
            return articles.count(article_query)

    def log_downloadable_articles_count(self):
        with self.lock:
            article_query = Query()
            article_query = article_query.crawl_status == 0
            articles = self.get_article_db()
            logging.info(f"Currently fetched articles ready to download: {articles.count(article_query)}")

    def create_article_object(self, article_id: str, language: str) -> dict:
        return {
            "type": self.article_type,
            "id": article_id,
            "language": language
        }

    def create_article_query(self, article_id: str, language: str) -> Query:
        query = Query()
        query = (query.type == self.article_type) & (query.id == article_id) & (query.language == language)
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

    def get_article_db(self):
        return self.db.table("articles")

    def get_website_db(self):
        return self.db.table("websites")

    def get_error_db(self):
        return self.db.table("download_errors")

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
