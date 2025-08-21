-- Databricks notebook source
-- MAGIC %md
-- MAGIC SETUP TABLES

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS default.creators_scrape_wiki(
  wiki_page STRING COMMENT "The wikipedia page for the creator"
)
USING DELTA
COMMENT "The wikipedia pages for creators"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS posts_creator(
  creator_id STRING COMMENT "The creator id",
  likes LONG COMMENT "The number of likes",
  published_at TIMESTAMP COMMENT "The time the post was published",
  tags ARRAY<STRING> COMMENT "The tags used in the post",
  title STRING COMMENT "The title of the post",
  views LONG COMMENT "The number of views",
  yt_user STRING COMMENT "The youtube user"
)
USING DELTA
COMMENT "The posts for creators"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS users_yt(
  user_id STRING COMMENT "The creator id",
  wiki_page STRING COMMENT "The creator wiki page"
)
USING DELTA
COMMENT "The users for creators"