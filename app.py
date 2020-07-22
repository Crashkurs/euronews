from api_crawler import EuroNewsCrawler
from api_processor import ApiProcessor
from page_crawler import PageCrawler
import logging
import schedule
import time
import os

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    working_dir = os.path.join("D:\\Projekte", "Python", "Test")

    # EuroNewsCrawler is responsible for delivering article metadata to the ApiProcessor
    crawler = EuroNewsCrawler(1, working_dir)
    start_dates = None  # [datetime.datetime(year=2020, month=1, day=1), datetime.datetime(year=2019, month=1, day=1)]
    crawler.start(start_dates)

    # ApiProcessor is responsible for filtering article metadata and create directories to store audio/text
    processor = ApiProcessor(working_dir)
    crawler.register_response_handler(processor.enqueue_response)

    # PageCrawler is responsible for actually crawling a single article and download text and audio
    page_crawler = PageCrawler(10)
    processor.register_page_handler(page_crawler.enqueue_new_page)

    schedule.every(10).minutes.do(crawler.start)  # schedule for loading new articles in the api
    schedule.every(10).seconds.do(crawler.persist_progress)  # schedule for persisting crawling progress
    schedule.every(5).seconds.do(processor.handle_responses)  # schedule for handling api responses
    schedule.every(5).seconds.do(page_crawler.crawl_next_pages)  # schedule for crawling articles and their videos
    while True:
        schedule.run_pending()  # schedule to start crawling every x minutes again
        time.sleep(1)
