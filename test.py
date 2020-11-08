import requests
from lxml import html, etree
from page_crawler import PageCrawler
from db import Database
import json
import logging

logging.basicConfig(level=logging.DEBUG, datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s [%(levelname)s]: %(message)s")
logging.getLogger("page_crawler").setLevel(logging.DEBUG)

class TestDB(Database):
    def __init__(self):
        pass

    def increment_crawled_article_status(self, article_id: str, language: str, amount: int = 1):
        pass

    def reset_crawled_article_status(self, article_id: str, language: str):
        pass

    def reset_crawled_articles_status(self):
        pass


class TestCrawler(PageCrawler):
    def store_text(self, id, language, root, output_file):
        logging.info(root)
        return

    def normal_download(self, video_url, output_dir):
        logging.info("normal: " + video_url)
        return

    def youtube_download(self, language, video_id, output_dir):
        logging.info("youtube: " + video_id)
        return

    def extract_video_ids(self, root_node) -> list:
        result = super(TestCrawler, self).extract_video_ids(root_node)
        logging.info(f"Extracted video ids: {result}")
        return result


def test_double_video_description():
    url = "https://hu.euronews.com/2020/07/29/tobb-szazmillio-ember-ivovizellatasat-oldhatja-meg-ez-a-kutyu"
    response = requests.get(url)
    crawler = TestCrawler(TestDB(), 1)
    crawler.request_context[url] = (id, "hu", ".")
    crawler.lock.acquire()
    crawler.handle_crawl_response(None, response)


test_double_video_description()
