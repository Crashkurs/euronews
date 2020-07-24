from api_crawler import Crawler
from db import Database
from queue import Queue
from lxml import html
from threading import Lock
import requests
import logging
import pytube
import os
import asyncio


class PageCrawler(Crawler):
    xpath_video_url = "//div[@class='js-player-pfp']/@data-video-id"
    xpath_article_content = "//div[contains(@class, 'c-article-content') and contains(@class, 'js-article-content article__content')]/p/text()"

    def __init__(self, database: Database, max_requests):
        super().__init__(max_requests)
        self.db = database
        self.lock = Lock()

    def crawl_next_pages(self):
        logging.info(f"Crawling {self.page_queue.qsize()} pages..")
        [print(x) for x in self.page_queue.queue]
        article = self.db.get_article_to_crawl()
        while article is not None:
            url, output_dir = article
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0"}
            self.add_request("GET", url, lambda session, response: self.handle_crawl_response(output_dir, response),
                             {}, headers)
            article = self.db.get_article_to_crawl()

    def handle_crawl_response(self, output_dir: str, response: requests.Response):
        self.store_response(output_dir, response)
        return response

    def store_response(self, output_dir: str, response: requests.Response):
        root_node = html.fromstring(response.content)
        video_urls = root_node.xpath(self.xpath_video_url)
        if len(video_urls) == 0:
            return
        audio_dir = output_dir
        text_file = os.path.join(output_dir, "article.txt")
        article_io = asyncio.run(self.store_text(root_node, text_file))
        audio_io = asyncio.run(self.download_video(root_node, audio_dir))
        return response

    async def store_text(self, root, output_file):
        article = "".join(root.xpath(self.xpath_article_content))
        with open(output_file, "w") as f:
            f.write(article)

    async def download_video(self, root, output_dir):
        video_urls = root.xpath(self.xpath_video_url)
        if len(video_urls) > 0:
            video_url = f"http://youtube.com/watch?v={video_urls[0]}"
            print(f"Downloading {video_url}")
            pytube.YouTube(video_url).streams\
                .filter(only_audio=True).first()\
                .download(output_path=output_dir, filename="audio")