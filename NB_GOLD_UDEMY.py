# Databricks notebook source
df_udemy_silver = spark.read.table("silver.udemy2")


# COMMAND ----------


# spark.sql("REFRESH TABLE silver.udemy2")

display(df_udemy_silver)

# COMMAND ----------

# MAGIC %md # top 5 courses which is enrolled high in few years

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,latest_enroll-enrollment as incresed_count from (
# MAGIC select link,title,enrollment,category,latest_enroll from silver.udemy2)x
# MAGIC order by incresed_count desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select category,sum(incre) increse_in_enrolment from(
# MAGIC select title,enrollment,category,latest_enroll,latest_enroll-enrollment as incre
# MAGIC from silver.udemy2)x
# MAGIC group by category
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select author_name,latest_enroll,category from (
# MAGIC select distinct (author_name),latest_enroll,instructor_rating,category,row_number()over(partition by category order by latest_enroll desc ) as rn from silver.udemy2)x
# MAGIC where rn<=5
# MAGIC order by latest_enroll desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC select author_name,category,sum(latest_enroll) over (partition by category order by latest_enroll desc) total_st ,
# MAGIC row_number() over(partition by category order by latest_enroll desc) as rn
# MAGIC from silver.udemy2)x
# MAGIC where rn=1
# MAGIC order by total_st desc
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import desc
report_df = df_udemy_silver.select("Title", "category", "Rating").orderBy(desc("Rating")).limit(10)
display(report_df)


# COMMAND ----------

# MAGIC %md # Total entrolments

# COMMAND ----------

# MAGIC %md #categorywise enrolment report

# COMMAND ----------

# from pyspark.sql.functions import sum
# report_df = df_udemy_silver.groupBy("category").agg(sum("Enrollment").alias("total_enrollment"))
# display(report_df)



from pyspark.sql.functions import avg
report_df = df_udemy_silver.groupBy("category").agg(sum("Enrollment").alias("total_enrollments"), avg("Stars").alias("avg_stars"))
display(report_df)


# COMMAND ----------

df_udemy_silver.printSchema()

# COMMAND ----------

# MAGIC %md #top rated and most percentage user ratings given

# COMMAND ----------

# from pyspark.sql.functions import round, col

# x=df_udemy_silver.select("title",'Enrollment','Rating','Stars')

# df = x.withColumn("percent_rating", round(col("rating")/col("Enrollment")*100,2)).orderBy(desc("percent_rating")).limit(10)
# re=df.select("title","Stars","percent_rating")
# display(re)


from pyspark.sql.functions import round, col,row_number
from pyspark.sql.window import Window

y=df_udemy_silver.select("title",'Enrollment','Rating','Stars','category','img_url')

df = y.withColumn("percent_rating", round(col("rating")/col("Enrollment")*100,2))
re=df.select("title","Stars","percent_rating",'category','img_url')


window_spec = Window.partitionBy('category').orderBy(col('percent_rating').desc())

df_with_ranks = re.withColumn('rank', row_number().over(window_spec))
df_top_5 = df_with_ranks.filter(col('rank') <= 1)
display(df_top_5)


# COMMAND ----------

# MAGIC %sql
# MAGIC select  author_name,category,sum(total_courses) as total_courses from silver.udemy2
# MAGIC group by author_name,category
# MAGIC order by total_courses desc
# MAGIC limit (5)

# COMMAND ----------

# MAGIC %md #course performance top and bottom  courses categorywise

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df = spark.table("silver.udemy2")

w = Window.partitionBy('category').orderBy(df['Enrollment'].desc())

df = df.withColumn('enrollment_rank_desc', row_number().over(w))
df = df.withColumn('enrollment_rank_asc', row_number().over(w.orderBy(df['Enrollment'].asc())))

df = df.filter((df['enrollment_rank_desc'] <= 1) | (df['enrollment_rank_asc'] <= 1))
df = df.orderBy(df['Enrollment'].desc())
# df=df.select("Enrollment",'category','title')
display(df)


# COMMAND ----------

# MAGIC %md #top5 courses based on enrolment in both categories

# COMMAND ----------


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df = spark.table("silver.udemy2")

w = Window.partitionBy('category').orderBy(df['Enrollment'].desc())

df = df.withColumn('enrollment_rank_desc', row_number().over(w))
df = df.withColumn('enrollment_rank_asc', row_number().over(w.orderBy(df['Enrollment'].asc())))

df = df.filter((df['enrollment_rank_desc'] <= 1))
df = df.orderBy(df['Enrollment'].desc())
# df=df.select("Enrollment",'category','title')
display(df)

# COMMAND ----------

# MAGIC %md #improvement percentage

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,latest_rating-stars as improve_ment from (
# MAGIC select title,stars,course_rating as latest_rating from silver.udemy2)x
# MAGIC order by improve_ment asc

# COMMAND ----------

# MAGIC %md #Giving sugestions based on user intrest

# COMMAND ----------

from pyspark.sql.functions import col,lower
input_str = input("Enter string to search: ")
result_df = df_udemy_silver.filter(lower(col('title')).like(f'%{input_str}%'))
result_df = result_df.orderBy('latest_enroll', ascending=False).limit(5)
display(result_df)


# COMMAND ----------


