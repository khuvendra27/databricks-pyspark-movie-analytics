# Databricks notebook source
# MAGIC %md
# MAGIC 1) Importing Files

# COMMAND ----------

from pyspark.sql.functions import col, split, explode, count, avg, year, from_unixtime

movies_path = "/Volumes/workspace/default/filestore/tables/ml25m/movies.csv"
tags_path = "/Volumes/workspace/default/filestore/tables/ml25m/tags.csv"
ratings_path = "/Volumes/workspace/default/filestore/tables/ml25m/ratings.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Bronze layer Raw Data 
# MAGIC Loading Datasets
# MAGIC

# COMMAND ----------

movies = spark.read.format('csv').option("inferSchema",True).option("header",True).load(movies_path)
ratings = spark.read.format('csv').option("inferSchema",True).option("header",True).load(ratings_path)
tags = spark.read.format('csv').option("inferSchema",True).option("header",True).load(tags_path)




# COMMAND ----------

# MAGIC %md
# MAGIC Write raw data to Delta

# COMMAND ----------

ratings.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/filestore/delta/bronze/ratings")
movies.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/filestore/delta/bronze/movies")
tags.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/filestore/delta/bronze/tags")

# COMMAND ----------

# MAGIC %md
# MAGIC 3) Silver Layer (Transformations)

# COMMAND ----------

movies_silver = movies.withColumn('genres',explode(split(col('genres'),"\\|")))
movies_silver.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/filestore/delta/silver/movies")

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Gold Layer (Aggregations)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC a. Top Movies (with at least 1000 ratings)
# MAGIC

# COMMAND ----------

ratings_movie = ratings.join(movies_silver,"movieId")

top_movies = ratings_movie.groupBy("movieId","title")\
    .agg(count("rating").alias("num_ratings") , avg("rating").alias("avg_rating"))\
    .where("num_ratings > 1000")\
    .orderBy(col("avg_rating").desc())\
    .limit(20)

top_movies.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/filestore/delta/gold/top_movies")
top_movies.write.format("delta").mode("overwrite").saveAsTable("top_movies")


# COMMAND ----------

# MAGIC %md
# MAGIC b.Popular Genres

# COMMAND ----------

popular_genres = ratings_movie.groupBy("genres")\
    .count()\
    .orderBy(col("count").desc())

popular_genres_by_ratings = ratings_movie.groupBy("genres")\
    .agg(count("rating").alias("num_ratings") , avg("rating").alias("avg_rating"))\
    .orderBy(col("avg_rating").desc())\
    .limit(20)

popular_genres.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/filestore/delta/gold/popular_genres")
popular_genres_by_ratings.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/filestore/delta/gold/popular_genres_by_ratings")

popular_genres_by_ratings.write.format("delta").mode("overwrite").saveAsTable("popular_genres_by_ratings")

# COMMAND ----------

# MAGIC %md
# MAGIC Query Delta Tables with SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from popular_genres_by_ratings ORDER BY avg_rating LIMIT 10 ;
# MAGIC SELECT * FROM top_movies ORDER BY avg_rating LIMIT 10 
# MAGIC