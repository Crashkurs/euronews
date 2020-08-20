from api_crawler import EuroNewsCrawler
from api_processor import ApiProcessor
from page_crawler import PageCrawler
from db import Database
import logging
import schedule
import time
import os


def log_downloaded_articles(db: Database):
    db.log_downloadable_articles_count()


if __name__ == '__main__':
    logging.basicConfig(filename="crawler.log",
        level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
        format="%(asctime)s [%(levelname)s]: %(message)s")
    logging.getLogger("schedule").setLevel(logging.WARN)
    logging.getLogger("page_crawler").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)
    logging.getLogger("youtube").setLevel(logging.ERROR)
    logging.getLogger("asyncio").setLevel(logging.WARN)
    working_dir = os.path.join(".", "data")
    os.makedirs(working_dir, exist_ok=True)

    db = Database(working_dir)
    db.reset_crawled_articles_status()  # restart downloads from previous session

    # EuroNewsCrawler is responsible for delivering article metadata to the ApiProcessor
    crawler = EuroNewsCrawler(db, 1000, 1, working_dir)
    start_dates = None  # [datetime.datetime(year=2020, month=1, day=1), datetime.datetime(year=2019, month=1, day=1)]
    crawler.start(start_dates)

    # ApiProcessor is responsible for filtering article metadata and create directories to store audio/text
    processor = ApiProcessor(db, working_dir)
    crawler.register_response_handler(processor.enqueue_response)

    # PageCrawler is responsible for actually crawling a single article and download text and audio
    page_crawler = PageCrawler(db, 1)

    schedule.every(1).hours.do(crawler.start)  # schedule for loading new articles in the api
    schedule.every(10).seconds.do(crawler.persist_progress)  # schedule for persisting crawling progress
    schedule.every(10).seconds.do(page_crawler.crawl_next_pages)  # schedule for crawling articles and their videos
    schedule.every(1).minutes.do(lambda: log_downloaded_articles(db))  # schedule for crawling articles and their videos

    try:
        while True:
            schedule.run_pending()  # schedule to start crawling every x minutes again
            time.sleep(1)
    except KeyboardInterrupt:
        pass
