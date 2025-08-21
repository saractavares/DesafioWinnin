# Databricks notebook source
"""
Ler  o arquivo em  /Volumes/winnin_tech_test/default/winnin/wiki_pages.json.gz e cria a tabela delta default.creators_scrape_wiki  
"""
df = spark.read.format("json").load("/Volumes/winnin_tech_test/default/winnin/wiki_pages.json.gz")

df.write.format("delta").mode("overwrite").saveAsTable("default.creators_scrape_wiki")