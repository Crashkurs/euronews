import requests
from lxml import html, etree
import json
import youtube_dl
import re

xpath_video_url = ["//div[@class='js-player-pfp']/@data-video-id",
                   "//script[re:match(text(), 'contentUrl')]/text()"]
xpath_text = "//div[contains(@class, 'c-article-content') or contains(@class, 'js-article-content') or "\
                            "contains(@class,'article__content')]/p/text()"
regex_video_url = [""]
response = requests.get("https://tr.euronews.com/2020/07/30/bilisim-hukuk-uzman-prof-dr-yaman-akdeniz-sosyal-medya-duzenlemesini-euronews-e-degerlendi")

root = html.fromstring(response.content)
for xpath in xpath_video_url:
    print(xpath)
    found = root.xpath(xpath, namespaces={"re": "http://exslt.org/regular-expressions"})
    if len(found) > 0:
        print(found)
        article = json.loads(found[0])
        print(article)
        url = article["@graph"][0]["video"]["embedUrl"]
        print(url)
        matching_pos = re.search("[^/]+$", url)
        id = url[matching_pos.start():matching_pos.end()]
        yt_url = f"https://youtube.com/watch?v={id}"
        print(id)
        #youtube_dl.YoutubeDL().download([yt_url])

print(root.xpath(xpath_text))