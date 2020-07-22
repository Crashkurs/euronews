from queue import Queue, Empty
from api_crawler import Website
from threading import Lock
from typing import Callable, Any, AnyStr
import os
import json
import logging

class ApiProcessor:
    def __init__(self, working_dir_path: str):
        self.api_queue = Queue()
        assert os.path.isdir(working_dir_path), "path is not a directory"
        assert os.access(working_dir_path, os.W_OK), "directory not writeable"
        self.working_dir = working_dir_path
        self.page_handler = []
        self.lock = Lock()

    def register_page_handler(self, handler: Callable[[AnyStr, AnyStr], Any]):
        self.page_handler.append(handler)

    def enqueue_response(self, website: Website, response: dict):
        with self.lock:
            article_id = response["id"]
            if self.article_is_present(article_id):
                return
            self.api_queue.put((website, response))

    def article_is_present(self, article_id: str):
        for website, response in self.api_queue.queue:
            if response["id"] == article_id:
                return True
        return False

    def handle_responses(self):
        logging.info(f"Processing {self.api_queue.qsize()} api responses...")
        while not self.api_queue.empty():
            try:
                website, response = self.api_queue.get()
                self.handle_response(website, response)
                self.api_queue.task_done()
            except Empty:
                pass

    def handle_response(self, website: Website, response: dict):
        try:
            # the path of the article
            full_page_path = website.url + response["fullUrl"]
            storage_dir = self.get_persistent_file_path_for_response(website, response)
            for handler in self.page_handler:
                handler(full_page_path, storage_dir)
            os.makedirs(storage_dir, exist_ok=True)
            response_file = os.path.join(storage_dir, "meta.json")
            with open(response_file, "w+") as f:
                f.write(json.dumps(response, indent=4))
        except Exception as e:
            logging.error(f"[{website.language}] exception while handling {response}")
            logging.exception(e)

    def get_persistent_file_path_for_response(self, website: Website, response: dict):
        article_id = str(response["id"])
        article_dir = os.path.join(self.working_dir, website.language, article_id)
        return article_dir
