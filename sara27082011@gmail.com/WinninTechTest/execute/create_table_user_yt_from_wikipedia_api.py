# Databricks notebook source
from pyspark.sql import functions as F
import requests
from bs4 import BeautifulSoup
import re

# COMMAND ----------

"""
Usar a tabela default.creators_scrape_wiki para buscar na api da wikipedia o user_id do youtube de cada wiki_name

dica 1: utilizar o endpoint https://en.wikipedia.org/w/api.php
dica 2: utilizar parametros params = {"action": "parse","page": f"{page_name}","format": "json"}
"""


# COMMAND ----------

"""
Campos da tabela default.users_yt: user_id(extraido da wikipedia) e o wiki_page(da tabela default.creators_scrape_wiki)

Exemplo de 1 registro da tabela {'user_id': 'felipeneto', 'wiki_page': 'Felipe_Neto'}
"""
def insert_user_youtube(wiki_page: str, user_id: str):
    spark.sql(f"""
        INSERT INTO default.users_yt(user_id, wiki_page)
        VALUES('{user_id}', '{wiki_page}')
    """)

# COMMAND ----------

def get_youtube_user_id(wiki_name: str):
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "parse",
        "page": wiki_name,
        "format": "json",
        "prop": "text"
    }

    response = requests.get(url, params=params)
    data = response.json()

    if "parse" not in data:
        return None

    html_content = data["parse"]["text"]["*"]
    soup = BeautifulSoup(html_content, "html.parser")

    links = soup.find_all("a", href=True)
    for link in links:
        href = link["href"]
        if "youtube.com" in href:
            match = re.search(r"(user|channel|c)/([^/?&]+)", href)
            if match:
                return match.group(2)
    return None

df_wiki_page = spark.sql("SELECT wiki_page FROM default.creators_scrape_wiki")

for row in df_wiki_page.collect():
    wiki_page = row.wiki_page
    user_id = get_youtube_user_id(wiki_page)

    if user_id:
        # insere via SQL
        spark.sql(f"""
            INSERT INTO default.users_yt (user_id, wiki_page)
            VALUES ('{user_id}', '{wiki_page}')
        """)
