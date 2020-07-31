import youtube_dl

from api_crawler import Crawler
from db import Database
from lxml import html
from threading import BoundedSemaphore
import requests
import logging
import os
import json
import re
from concurrent import futures


class PageCrawler(Crawler):
    youtube_url = "https://youtube.com/watch?v="
    xpath_video_url = ["//div[@class='js-player-pfp']/@data-video-id", "//script[re:match(text(), 'contentUrl')]/text()"]
    xpath_article_content = "//div[contains(@class, 'c-article-content') or contains(@class, 'js-article-content') or "\
                            "contains(@class,'article__content')]/p/text()"
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
        self.get_logger().debug(f"Crawling next {self.lock._value} available pages..")
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
                    self.lock.release()
                    return

    def handle_crawl_response(self, session, response: requests.Response):
        try:
            request = response.request
            url = request.url
            if url not in self.request_context:
                self.get_logger().warning(f"{url} does not have a context")
                return
            id, language, output_dir = self.request_context[url]
            del self.request_context[url]
            self.store_response(id, language, output_dir, response)
        except Exception as e:
            self.get_logger().exception(e)
        self.lock.release()
        return response

    def store_response(self, id: str, language: str, output_dir: str, response: requests.Response):
        root_node = html.fromstring(response.content)
        video_ids = []
        for xpath_url in self.xpath_video_url:
            extracted_ids = root_node.xpath(xpath_url, namespaces={"re": "http://exslt.org/regular-expressions"})
            if len(extracted_ids) > 0:
                video_ids = extracted_ids
        if len(video_ids) == 0:
            self.get_logger().debug("[%s] No video in article %s in dir %s", language, id, output_dir)
            self.db.increment_crawled_article_status(id, language, 2)  # mark this article as finished in db
            return
        audio_dir = output_dir
        text_file = os.path.join(output_dir, "article.txt")
        video_id = self.prepare_video_id(video_ids)
        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            text = executor.submit(self.store_text, id, language, root_node, text_file)
            video = executor.submit(self.download_video, id, language, video_id, audio_dir)
            text.result()
            video.result()
        return response

    def store_text(self, id, language, root, output_file):
        try:
            article = " ".join(root.xpath(self.xpath_article_content))
            if len(article) == 0:
                self.get_logger().warning(f"[{language}] No article for {id} was downloaded")
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(article)
            self.db.increment_crawled_article_status(id, language)
        except Exception as e:
            self.get_logger().exception(e)

    def prepare_video_id(self, video_ids: list):
        for video_id in video_ids:
            if len(video_id) == 11:  # if the id has length 11, it is a proper youtube video id and we can start to download
                return video_id
            if "{" in video_id and "}" in video_id:  # if we have a json string, parse and search the content of it for the video id
                json_content = json.loads(video_id)
                if "@graph" in json_content:
                    json_content = json_content["@graph"]
                if len(json_content) > 0:
                    json_content = json_content[0]
                if "video" in json_content:
                    json_content = json_content["video"]
                if "embedUrl" in json_content and len(json_content["embedUrl"]) > 0:
                    json_content = json_content["embedUrl"]
                    matching_pos = re.search("[^/]+$", json_content)
                    if matching_pos is not None:
                        return json_content[matching_pos.start():matching_pos.end()]
                if "contentUrl" in json_content and len(json_content["contentUrl"]) > 0:
                    return json_content["contentUrl"]

        return video_ids[0]

    def download_video(self, id: str, language: str, video_id: str, output_dir):
        try:
            # if the fetched video starts with an https, we did not find a youtube video id, but a full url
            if "https" in video_id and (".mp3" in video_id or ".mp4" in video_id):
                self.get_logger().debug(f"Normal download of {video_id}")
                self.normal_download(video_id, output_dir)
            else:
                self.get_logger().debug(f"Youtube download of {video_id}")
                self.youtube_download(language, video_id, output_dir)
            self.db.increment_crawled_article_status(id, language)
        except Exception as e:
            self.get_logger().exception(e)

    def youtube_download(self, language, video_id, output_dir):
        url = f"{self.youtube_url}{video_id}"
        if language == "www":
            language = "en"
        download_properties = self.youtube_dl_properties.copy()
        download_properties["outtmpl"] = f'{output_dir}/audio.mp3'
        download_properties["subtitleslangs"] = [language]
        tube = youtube_dl.YoutubeDL(download_properties)
        tube.download([url])

    def normal_download(self, video_url, output_dir):
        response = requests.get(video_url)
        if response.status_code != 200:
            return
        with open(os.path.join(output_dir, "audio.mp3"), "wb") as f:
            f.write(response.content)

    def get_logger(self):
        return logging.getLogger("page_crawler")
