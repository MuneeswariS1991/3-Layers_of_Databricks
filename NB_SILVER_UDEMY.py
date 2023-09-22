# Databricks notebook source

src_final_df = spark.read.table("bronze.udemy1")



# COMMAND ----------

# # display(src_final_df)
# dbutils.fs.rm("dbfs:/mini_project/DataLake/silver/udemy", True)
src_final_df.write.format("parquet").mode("overwrite").saveAsTable("silver.udemy2")



# COMMAND ----------

# src_final_df = spark.read.table("bronze.udemy1")

# COMMAND ----------

from pyspark.sql.functions import col
src_final_df = src_final_df.withColumn("Enrollment", col("Enrollment").cast("integer"))
src_final_df = src_final_df.dropna(how="any")

# COMMAND ----------

display(src_final_df)

# COMMAND ----------

pip install beautifulsoup4

# COMMAND ----------


import requests
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from bs4 import BeautifulSoup
import requests

def fetch_data_from_url(url):
    data = requests.get(url)
    try:
        array=[]
        s = BeautifulSoup(data.text, 'html.parser')
        instructor_tag=s.find_all('div',{"ud-text-md instructor--instructor__job-title--3iHjg"})[0].text.replace(","," ")
        written_rating=s.find_all('div',{'class':"ud-block-list-item-content"})[-3].text.replace(",","")
        total_students=s.find_all('div',{'class':"ud-block-list-item-content"})[-2].text.replace(",","")
        total_courses=s.find_all('div',{'class':"ud-block-list-item-content"})[-1].text.replace(",","")
        instructor_rating=s.find_all('div',{'class':"ud-block-list-item-content"})[-4].text.replace(",","")
        latest_st_enroll=s.find_all("div",{'class':"enrollment"})[0].text.replace(",","")
        array.append(instructor_tag)
        array.append(written_rating)
        array.append(total_students)
        array.append(latest_st_enroll)
        array.append(total_courses)
        array.append(instructor_rating)
        images = s.find_all('img')
        course_images=[]
        for image in images:
            image_url = image['src']
            course_images.append(image_url)
        
        array.append(course_images[1])
        elements=s.find_all('span')
        
        z=[]
        for i in elements:
            z.append(i.text.replace(",",""))
        e=[]
        z = list(filter(lambda y: y != '', z))
#         e.append(z[5])
        e.append(s.find_all('span',{'class':"ud-heading-sm star-rating-module--rating-number--2xeHu"})[0].text.replace(",",""))
        e.append(z[6])
#         e.append(z[9])
        e.append(s.find_all('a',{'class':'ud-btn ud-btn-large ud-btn-link ud-heading-md ud-text-sm ud-instructor-links'})[0].text.replace(",",""))
        e.append(s.find_all('div',{'class':"last-update-date"})[0].text)

        all_content=", ".join(e)
        
        array.append(all_content)
        
#         return array
    except IndexError:
           array=[None,None,None,None,None,None,None,None,None,None,None]
#         instructor_tag=None
#         written_rating = None
#         instructor_tag=None
#         total_students = None
#         total_courses=None
#         instructor_rating = None
#         array.append(instructor_tag)
#         array.append(written_rating)
#         array.append(total_students)
#         array.append(total_courses)
#         array.append(instructor_rating)
#         array.append(None)
    return array
  
# url='https://www.udemy.com/course/the-complete-sql-bootcamp/'
# print(fetch_data_from_url (url))   

fetch_data_udf = udf(fetch_data_from_url, StringType())
src_final_df = src_final_df.withColumn("data", fetch_data_udf(col("Link")))


# COMMAND ----------

# display(src_final_df)
src_final_df.write.format("parquet").mode("overwrite").saveAsTable("silver.udemy_raw")



# COMMAND ----------

# club_data=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mnt/export__2_.csv")

club_data=src_final_df

# COMMAND ----------

from pyspark.sql.functions import split, col

split_df = club_data.select("index","Title","Summary","Enrollment","Stars","Rating","Link","category", split(col("data"), ",").alias("data"))

# extract values from the array
result_df = split_df.select("index","Title","Summary","Enrollment","Stars","Rating","Link","category",
                            col("data").getItem(0).alias("instructor_tag"), 
                            col("data").getItem(1).alias("written_rating"),
                            col("data").getItem(2).alias("total_students_author"),
                            col("data").getItem(3).alias("latest_enroll"),
                            col("data").getItem(4).alias("total_courses"),
                            col("data").getItem(5).alias("instructor_rating"),
                            col("data").getItem(6).alias("img_url"),
                            col("data").getItem(7).alias("course_rating"),
                            col("data").getItem(8).alias("written_course_rating"),
                            col("data").getItem(9).alias("author_name"),
                            col("data").getItem(10).alias("last_modified")
                           )



# COMMAND ----------

display(result_df)
# result_df=result_df.drop('total_students')

# COMMAND ----------

from pyspark.sql.functions import udf,substring
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
from pyspark.sql.functions import length
from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import StringType
import re
from pyspark.sql.functions import current_timestamp

# get_integers_before_space = udf(lambda s:r'\d+' if s else None, IntegerType())

# add new column with existing column
# result_df = result_df.withColumn('lens', length(result_df['written_rating']))

result_df= result_df.withColumn('written_rating', regexp_extract(result_df['written_rating'], '^\\D*(\\d+)', 1))


def collect_only_digits_before_space(value):
    import re
    pattern = r'^\D*(\d+)'
    match = re.search(pattern, str(value))
    if match:
        return match.group(1)
    else:
        return None
    

def extract_month_year(value):
    pattern = r'Last updated (\d{1,2}/\d{4})'
    match = re.search(pattern, str(value))
    if match:
        return match.group(1)
    else:
        return None


my_udf = udf(collect_only_digits_before_space, StringType())
extract_month_year_udf = udf(extract_month_year, StringType())

result_df = result_df.withColumn('total_students_author', my_udf(result_df['total_students_author']))
result_df = result_df.withColumn('latest_enroll', my_udf(result_df['latest_enroll']))
# latest_enroll
# result_df = result_df.withColumn('total_students', udf(my_udf, IntegerType())(result_df['total_students']).cast('integer'))
result_df = result_df.withColumn('total_courses', my_udf(result_df['total_courses']))
result_df = result_df.withColumn('instructor_rating', substring(result_df['instructor_rating'], 1, 4))
result_df = result_df.withColumn('written_course_rating', my_udf(result_df['written_course_rating']))
result_df = result_df.withColumn('last_modified', extract_month_year_udf('last_modified'))
result_df = result_df.withColumn("created_timestamp", current_timestamp())




# COMMAND ----------

silver_udemy_schema=[["index","int" ]
,["Title" ,"string" ]
,["Summary","string" ]
,["Enrollment","int" ]
,["Stars","double"]
,["Rating","int" ]
,["Link","string" ]
,["category","string" ]
,["instructor_tag","string" ]
,["written_rating","int" ]
,["total_students_author","int" ]
 ,["latest_enroll","int" ]                  
,["total_courses","int" ]
,["instructor_rating","double"]
,["img_url","string" ]
,["course_rating","double"]
,["written_course_rating","int" ]
,["author_name","string" ]
,["last_modified","string"]
,["created_timestamp","timestamp"]]

# COMMAND ----------

from pyspark.sql.functions import to_date,when,lpad
df_udemy_silver = result_df
for column, datatype in silver_udemy_schema:
  df_udemy_silver = df_udemy_silver.withColumn(column, col(column).cast(datatype))

df_udemy_silver = df_udemy_silver.withColumn("last_modified", lpad("last_modified", 7, "0"))

# COMMAND ----------

display(df_udemy_silver)

# COMMAND ----------

# movie_name = input("Enter the name of the movie: ") 
# try:
#     user_search = df_udemy_silver.filter(df_udemy_silver.Title == movie_name).select("Genre").collect()[0][0]`
#     genre =genre.split("/")    
#     related_courses = df_udemy_silver.filter(df_udemy_silver.Title == 
# except IndexError :
#     print('there is only movie related to that genre') 
# display(related_courses)

# COMMAND ----------

df_udemy_silver.write.format("parquet").mode("overwrite").saveAsTable("silver.udemy_all_string")
# df_udemy_silver = df_udemy_silver.withColumn("last_modified", to_date("last_modified", "MM/yyyy"))

# COMMAND ----------

df_udemy_silver = spark.read.table("silver.udemy_all_string")

# COMMAND ----------

display(df_udemy_silver)


# COMMAND ----------

df_udemy_silver=df_udemy_silver.filter(df_udemy_silver.written_course_rating>4)

# COMMAND ----------

df_udemy_silver.write.format("parquet").mode("overwrite").saveAsTable("silver.udemy2")

# COMMAND ----------

display(df_udemy_silver)

# COMMAND ----------


