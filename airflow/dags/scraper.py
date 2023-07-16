import requests
from bs4 import BeautifulSoup


def download_page(url: str) -> str:
    response = requests.get(url)
    return response.text


def parse_patent(html: str):
    soup = BeautifulSoup(html, "lxml")
    number = (
        soup.find("table", class_="tp")
        .find("div", id="top4")
        .text[1:-1]
        .replace(" ", "")
    )
    link = f"https://new.fips.ru/registers-doc-view/fips_servlet?DB=RUPAT&DocNumber={number}&TypeFile=html"
    abstract = soup.find("div", id="Abs").findAll("p")[1].text[:-1]

    return number, link, abstract
