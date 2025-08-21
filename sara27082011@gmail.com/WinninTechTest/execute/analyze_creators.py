# Databricks notebook source
from pyspark.sql import functions as F, Window

# COMMAND ----------

"""
Usar o join da default.users_yt com a default.posts_creator para gerar analises desses creators

"""
df_users_yt = spark.table("default.users_yt")
df_posts_creator = spark.table("default.posts_creator")

df_join = (df_users_yt.join(df_posts_creator, df_users_yt.user_id == df_posts_creator.yt_user))


# COMMAND ----------

# DBTITLE 1,Top 3 por Likes
"""Mostrar o top 3 posts ordenado por likes de cada creator nos últimos 6 meses (user_id, title, likes, rank)"""

df_top3_likes = (df_join
    .filter(df_join.published_at >= F.add_months(F.current_date(), -24)) # apesar do teste pedir 6 meses, não há dados atualizados nesse range por isso estou usando 24 meses
    .withColumn("rank", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("likes"))))
    .filter(F.col("rank") <= 3)
    .select("user_id", "title", "likes", "rank"))
df_top3_likes.display()


# COMMAND ----------

# DBTITLE 1,Top 3 por views dos ultimos 6 meses
"""Mostrar o top 3 posts ordenado por views de cada creator nos últimos 6 meses (user_id, title, views, rank)"""
df_top3_views = (df_join
    .filter(df_join.published_at >= F.add_months(F.current_date(), -24)) # apesar do teste pedir 6 meses, não há dados atualizados nesse range por isso estou usando 24 meses
    .withColumn("rank", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("views"))))
    .filter(F.col("rank") <= 3)
    .select("user_id", "title", "views", "rank"))
df_top3_views.display()


# COMMAND ----------

# DBTITLE 1,users em post_creator mas não em users_yt
"""Mostrar os yt_user que estão na tabela default.post_creator mas não estão na tabela default.users_yt"""
df_extra = df_posts_creator.join(df_users_yt, df_posts_creator.yt_user == df_users_yt.user_id, "leftanti")
df_extra.display()

# COMMAND ----------

# DBTITLE 1,posts por mes por creator, sub null por 0
"""Mostrar a quantidade de publicações por mês de cada creator, mostrar 0 nos meses que não tem video"""

# lista de meses (1 a 12)
months = spark.createDataFrame([(m,) for m in range(1, 13)], ["month"])

# todos os users distintos
users = df_join.select("user_id").distinct()

# todos os anos distintos
years = df_join.select(F.year("published_at").alias("year")).distinct()

# grid completo (ano x mês x user)
full = years.crossJoin(months).crossJoin(users)

# contagem real
counts = (
    df_join.groupBy(
        F.year("published_at").alias("year"),
        F.month("published_at").alias("month"),
        "user_id"
    )
    .count()
)

# junta e preenche com 0
result = (
    full.join(counts, ["year", "month", "user_id"], "left")
    .fillna(0, subset=["count"])
)

result.display()

# COMMAND ----------

"""
Exercício Extra 2: transformar a tabela no formato que a primeira coluna é o user_id e temos uma coluna para cada mês.
ex:
user_id, 2024/01, 2024/02, 2024/03
felipeneto, 10, 20, 30
lucasneto, 5, 10, 15
"""
# cria coluna ano/mês no formato yyyy/MM
df_pivot = result.withColumn("year_month", 
                        F.concat_ws("/", 
                                    F.col("year"), 
                                    F.lpad(F.col("month"), 2, "0")))

# pivot: transforma meses em colunas
pivoted = (
    df_pivot.groupBy("user_id")
       .pivot("year_month")
       .agg(F.first("count"))
       .fillna(0)
)

pivoted.display()


# COMMAND ----------

"""Exercício Extra 3: Mostrar as 3 tags mais utilizadas por criador de conteúdo"""
df_tags = df_join.select("tags", "yt_user").withColumn("tags", F.explode("tags")).select("tags", "yt_user")

df_tags.groupBy("yt_user","tags").count().withColumn("rank",F.row_number().over(Window.partitionBy("yt_user").orderBy(F.desc("count")))).filter("rank<=3").display()