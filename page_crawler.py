import youtube_dl

from api_crawler import Crawler
from db import Database
from lxml import html
from threading import BoundedSemaphore
import requests
import logging
import os
from concurrent import futures


class PageCrawler(Crawler):
    youtube_url = "https://youtube.com/watch?v="
    xpath_video_url = "//div[@class='js-player-pfp']/@data-video-id"
    xpath_article_content = "//div[contains(@class, 'c-article-content') and contains(@class, 'js-article-content') and "\
                            "contains(@class,'article__content']/p/text()"
    youtube_dl_properties = {
        "extractaudio": True,
        "format": "251",  # webm with high quality
        "audioformat": "mp3",
        "writesubtitles": True,
        "writeautomaticsub": True,
        "quiet": True,
        "logger": logging.getLogger("youtube")
    }

    def __init__(self, database: Database, max_requests):
        super().__init__(max_requests)
        self.max_requests = max_requests
        self.db = database
        self.lock = BoundedSemaphore(max_requests)
        self.request_context = {}

    def crawl_next_pages(self):
        self.get_logger().info(f"Crawling next {self.lock._value} available pages..")
        for i in range(self.lock._value):
            # only start downloading an article if the semaphore has free capacity to reduce bloating the requests queue
            if self.lock.acquire(blocking=False):
                article = self.db.get_article_to_crawl()
                if article is not None:
                    id, language, url, output_dir = article
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0"}
                    self.request_context[url] = (id, language, output_dir)
                    self.add_request("GET", url,
                                     self.handle_crawl_response,
                                     {}, headers)
                else:
                    self.get_logger().info("No articles left to crawl")

    def handle_crawl_response(self, session, response: requests.Response):
        request = response.request
        url = request.url
        if url not in self.request_context:
            self.get_logger().warning(f"{url} does not have a context")
            return
        id, language, output_dir = self.request_context[url]
        del self.request_context[url]
        self.store_response(id, language, output_dir, response)
        return response

    def store_response(self, id: str, language: str, output_dir: str, response: requests.Response):
        root_node = html.fromstring(response.content)
        video_ids = root_node.xpath(self.xpath_video_url)
        if len(video_ids) == 0:
            self.get_logger().debug("[%s] No video in article %s in dir %s", language, id, output_dir)
            self.db.increment_crawled_article_status(id, language, 2)  # mark this article as finished in db
            self.lock.release()
            return
        audio_dir = output_dir
        text_file = os.path.join(output_dir, "article.txt")
        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            text = executor.submit(self.store_text, id, language, root_node, text_file)
            video = executor.submit(self.download_video, id, language, video_ids[0], audio_dir)
            video.result()
            text.result()
        return response

    def store_text(self, id, language, root, output_file):
        article = " ".join([x.strip() for x in root.xpath(self.xpath_article_content)])
        self.get_logger().info(f"store {article}")
        with open(output_file, "w") as f:
            f.write(article)
        self.db.increment_crawled_article_status(id, language)

    def download_video(self, id: str, language: str, video_id: str, output_dir):
        url = f"{self.youtube_url}{video_id}"
        if language == "www":
            language = "en"
        download_properties = self.youtube_dl_properties.copy()
        download_properties["outtmpl"] = f'{output_dir}/audio.mp3'
        download_properties["subtitleslangs"] = [language]
        tube = youtube_dl.YoutubeDL(download_properties)
        tube.download([url])
        self.lock.release()  # free semaphore to start another download
        self.db.increment_crawled_article_status(id, language)

    def get_logger(self):
        return logging.getLogger("page_crawler")
