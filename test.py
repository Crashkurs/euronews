import requests
from lxml import html, etree
import youtube_dl
import logging
from functools import partial

def download_progress(stream, bytes_left):
    max_bytes = stream.filesize
    logging.info(f"{(max_bytes-bytes_left)/1024}/{max_bytes/1024}")

def download_complete(id, language, stream, file):
    logging.info(f"Downloaded {file}")


video_url = "https://www.youtube.com/watch?v=TrdmCkmK3y4"
language = "www"
output_dir = "test"
properties = {
    "outtmpl": f'{output_dir}/audio2.%(ext)s',
    "listformats": True,
    "extractaudio": True,
    "format": "250",
    "audioformat": "mp3",
    "writesubtitles": True,
    "writeautomaticsub": True,
    "subtitleslangs": ["en"]
}
tube = youtube_dl.YoutubeDL(properties)
tube = tube.download([video_url])
print("test")