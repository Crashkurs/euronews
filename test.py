import requests
from lxml import html, etree
import json
import youtube_dl
import re

xpath_video_url = ["//div[@class='js-player-pfp']/@data-video-id",
                   "//script[re:match(text(), 'contentUrl')]/text()"]
xpath_text = "//div[contains(@class, 'c-article-content') or contains(@class, 'js-article-content') or " \
             "contains(@class,'article__content')]/p/text()"
regex_video_url = [""]
response = requests.get("https://pt.euronews.com/2019/07/02/presidente-filipe-nyusi-inicia-visita-de-estado-a-portugal")

root = html.fromstring(response.content)
for xpath in xpath_video_url:
    print(xpath)
    found = root.xpath(xpath, namespaces={"re": "http://exslt.org/regular-expressions"})
    if len(found) > 0 and xpath == xpath_video_url[1]:
        print(len(found))
        article = json.loads(found[0])
        print(json.dumps(article, indent=4))
        url = article["@graph"][0]["video"]["embedUrl"]
        print(url)
        matching_pos = re.search("[^/]+$", url)
        id = url[matching_pos.start():matching_pos.end()]
        yt_url = f"https://youtube.com/watch?v={id}"
        print(id)
        # youtube_dl.YoutubeDL().download([yt_url])
    if len(found) > 0:
        print(found[0])
        break

print(root.xpath(xpath_text))
