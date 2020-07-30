from api_crawler import EuroNewsCrawler
from api_processor import ApiProcessor
from page_crawler import PageCrawler
from db import Database
import logging
import schedule
import time
import os

if __name__ == '__main__':
    logging.basicConfig(filename="crawler.log", level=logging.DEBUG, datefmt="%Y-%m-%d %H:%M:%S",
                        format="%(asctime)s [%(levelname)s]: %(message)s")
    logging.getLogger("schedule").setLevel(logging.WARN)
    logging.getLogger("page_crawler").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.DEBUG)
    logging.getLogger("youtube").setLevel(logging.ERROR)
    working_dir = os.path.join(".", "data")
    os.makedirs(working_dir, exist_ok=True)

    db = Database(working_dir)
    db.reset_crawled_article_status()  #

    # EuroNewsCrawler is responsible for delivering article metadata to the ApiProcessor
    crawler = EuroNewsCrawler(db, 4, working_dir)
    start_dates = None  # [datetime.datetime(year=2020, month=1, day=1), datetime.datetime(year=2019, month=1, day=1)]
    crawler.start(start_dates)

    # ApiProcessor is responsible for filtering article metadata and create directories to store audio/text
    processor = ApiProcessor(db, working_dir)
    crawler.register_response_handler(processor.enqueue_response)

    # PageCrawler is responsible for actually crawling a single article and download text and audio
    page_crawler = PageCrawler(db, 10)

    schedule.every(1).hours.do(crawler.start)  # schedule for loading new articles in the api
    schedule.every(10).seconds.do(crawler.persist_progress)  # schedule for persisting crawling progress
    #schedule.every(5).seconds.do(page_crawler.crawl_next_pages)  # schedule for crawling articles and their videos

    try:
        while True:
            schedule.run_pending()  # schedule to start crawling every x minutes again
            time.sleep(1)
    except KeyboardInterrupt:
        pass
