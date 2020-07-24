from api_crawler import Website
from db import Database
import os
import json
import logging
import asyncio


class ApiProcessor:
    def __init__(self, database: Database, working_dir_path: str):
        assert os.path.isdir(working_dir_path), "path is not a directory"
        assert os.access(working_dir_path, os.W_OK), "directory not writeable"
        self.working_dir = working_dir_path
        self.db = database

    def enqueue_response(self, website: Website, response: dict):
        asyncio.run(self.handle_response(website, response))

    async def handle_response(self, website: Website, response: dict):
        try:
            # the path of the article
            full_page_path = website.url + response["fullUrl"]
            storage_dir = self.get_persistent_file_path_for_response(website, response)
            os.makedirs(storage_dir, exist_ok=True)
            self.db.store_article(response["id"], website.language, full_page_path,
                                  storage_dir)  # store information for the page crawler in db
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
