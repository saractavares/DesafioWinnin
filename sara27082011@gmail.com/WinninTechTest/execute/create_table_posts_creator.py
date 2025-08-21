# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

"""
Ler  o arquivo em  /Volumes/winnin_tech_test/default/winnin/posts_creator.json.gz e cria a tabela delta default.posts_creator  
"""
df = (
    (
        spark.read.format("json").load(
            "/Volumes/winnin_tech_test/default/winnin/posts_creator.json.gz"
        )
    )
    .withColumn("published_at_ts", F.from_unixtime("published_at").cast("timestamp"))
    .drop("published_at")
    .withColumnRenamed("published_at_ts", "published_at")
)

df.write.format("delta").mode("overwrite").saveAsTable("default.posts_creator")