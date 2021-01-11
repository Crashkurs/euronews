import youtube_dl

from api_crawler import Crawler
from db import Database
from lxml import html
import requests
import logging
import os
import json
import schedule
import time
from concurrent import futures


class PageCrawler(Crawler):
    youtube_url = "https://youtube.com/watch?v="
    xpath_video_url = ["//div[@class='js-player-pfp']/@data-video-id",
                       "//iframe[contains(concat(' ', @class, ' '), ' js-livestream-player ')]/@data-src",
                       "//div[@id='jsMainMediaArticle']/@data-content",
                       "//div[@class='c-video-player']/@data-content"]
    xpath_article_content = "//div[contains(@class, 'c-article-content') or contains(@class, 'js-article-content') or " \
                            "contains(@class,'article__content')]/p/text()"
    youtube_dl_properties = {
        "extractaudio": True,
        "format": "251",  # webm with high quality
        "audioformat": "mp3",
        "writesubtitles": True,
        "writeautomaticsub": True,
        # "quiet": True,
        "logger": logging.getLogger("youtube"),
        "buffersize": 128,
        "ratelimit": 50000,
        "noresizebuffer": True,
        "sleep_interval": 4,
        "max_sleep_interval": 15,
        "cookiefile": "./cookies.txt"
    }

    def __init__(self, database: Database, max_requests, limit_bandwidth=True):
        super().__init__(max_requests)
        self.max_requests = max_requests
        self.db = database
        self.request_context = {}
        if not limit_bandwidth:
            del self.youtube_dl_properties["ratelimit"]

    def continue_with_next_page(self):
        self.crawl_next_pages()
        return schedule.CancelJob

    def crawl_next_pages(self):
        self.get_logger().debug(f"Crawling next available page..")
        id, language, url, output_dir = self.db.get_article_to_crawl()
        if id is not None:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0"}
            tuple = (id, language, url, output_dir)
            self.request_context[url] = tuple
            self.add_request("GET", url,
                             lambda session, response: self.handle_crawl_response(tuple, response),
                             {}, headers)
        else:
            self.get_logger().info(f"[{language}]No articles left to crawl")
            return
        time.sleep(2)

    def handle_crawl_response(self, request_context, response: requests.Response):
        language = ""
        output_dir = ""
        try:
            id, language, url, output_dir = request_context
            if url not in self.request_context:
                self.get_logger().warning(f"{url} does not have a context")
                self.crawl_next_pages()
                return
            del self.request_context[url]
            self.store_response(id, language, output_dir, response)
        except Exception as e:
            self.get_logger().warning(f"Exception for language {language} in directory {output_dir}")
            self.get_logger().exception(e)
            self.db.move_article_to_error_list(id, language)
        return response

    def store_response(self, id: str, language: str, output_dir: str, response: requests.Response):
        root_node = html.fromstring(response.content)
        video_ids = self.extract_video_ids(root_node)
        if len(video_ids) == 0:
            self.get_logger().debug("[%s] No video in article %s in dir %s", language, id, output_dir)
            self.db.increment_crawled_article_status(id, language, 2)  # mark this article as finished in db
            schedule.every(2).seconds.do(self.continue_with_next_page)
            return
        audio_dir = output_dir
        text_file = os.path.join(output_dir, "article.txt")
        video_id = self.prepare_video_id(video_ids, audio_dir)
        if video_id is None:
            self.get_logger().debug("[%s] No video in article %s in dir %s", language, id, output_dir)
            return response
        self.get_logger().info(f"[{language}] Downloading video for article {id}")
        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            text = executor.submit(self.store_text, id, language, root_node, text_file)
            video = executor.submit(self.download_video, id, language, video_id, audio_dir)
            text.result()
            video.result()
        return response

    def extract_video_ids(self, root_node) -> list:
        for xpath_url in self.xpath_video_url:
            extracted_ids = root_node.xpath(xpath_url, namespaces={"re": "http://exslt.org/regular-expressions"})
            if len(extracted_ids) > 0:
                return extracted_ids
        return []

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

    def prepare_video_id(self, video_ids: list, audio_dir):
        for video_id in video_ids:
            # if the id has length 11, it is a proper youtube video id and we can start to download
            if len(video_id) == 11:
                return video_id
            if "youtube.com/embed" in video_id:
                short_video_id = video_id.replace("http://", "")
                short_video_id = short_video_id.replace("https://", "")
                short_video_id = short_video_id.replace("www.", "")
                short_video_id = short_video_id.replace("youtube.com/embed/", "")
                return short_video_id
            # if we have a json string, parse and search the content of it for the video id
            if "{" in video_id and "}" in video_id:
                json_content = json.loads(video_id)
                if "@graph" in json_content:
                    json_content = json_content["@graph"]
                if "videos" in json_content:
                    json_content = json_content["videos"]
                if len(json_content) > 0:
                    json_content = json_content[0]
                if "url" in json_content and json_content["url"] is not None and len(json_content["url"]) > 0:
                    return json_content["url"]
                if "youtubeId" in json_content and json_content["youtubeId"] is not None and len(
                        json_content["youtubeId"]) > 0:
                    return json_content["youtubeId"]
        self.get_logger().warning(f"Selecting no video id because no xpath was matching")
        return None

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
        except youtube_dl.utils.ExtractorError as ee:
            # self.db.reset_crawled_article_status(id, language)
            self.get_logger().error(f"Could not open {self.youtube_url}{video_id} - maybe video is private")
            self.get_logger().exception(ee)
            self.db.move_article_to_error_list(id, language)
        except youtube_dl.utils.DownloadError as de:
            # self.db.reset_crawled_article_status(id, language)
            self.get_logger().error(f"Error while downloading {self.youtube_url}{video_id} with article id {id}")
            self.get_logger().exception(de)
            self.db.move_article_to_error_list(id, language)
        except Exception as e:
            self.db.reset_crawled_article_status(id, language)
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
