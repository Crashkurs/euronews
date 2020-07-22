import requests
from lxml import html, etree

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0"}
response = requests.get("https://www.euronews.com/2020/07/18/global-covid-19-roundup-footvolley-returns-to-rio-while-australia-suspends-parliament",
             headers=headers)

root = html.fromstring(response.content)
print("".join(root.xpath("//div[contains(@class, 'c-article-content') and contains(@class, 'js-article-content article__content')]/p/text()")))