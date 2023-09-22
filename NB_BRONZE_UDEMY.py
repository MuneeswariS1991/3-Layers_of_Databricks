# Databricks notebook source
# MAGIC %python
# MAGIC dbutils.fs.mkdirs("/FileStore/kaggle.json")
# MAGIC

# COMMAND ----------

path='dbfs:/FileStore/kaggle.json'
spark_credentials_df=spark.read.format('json').option('header','true').option('inferschema','true').load(path)

# COMMAND ----------

display(spark_credentials_df)

# COMMAND ----------

user_name=spark_credentials_df.select(spark_credentials_df.username).take(1)[0]['username']
key=spark_credentials_df.select(spark_credentials_df.key).take(1)[0]['key']

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install kaggle

# COMMAND ----------

def autunticate_kaggle(user_name,key):

    import os
    os.environ['KAGGLE_USERNAME']=user_name
    os.environ['KAGGLE_KEY']=key

    from kaggle.api.kaggle_api_extended import KaggleApi

    api=KaggleApi()
    api.authenticate()
    
    return api

# COMMAND ----------

autunticate_kaggle(user_name,key)

# COMMAND ----------

# MAGIC %sh
# MAGIC kaggle datasets download -d songseungwon/2020-udemy-courses-dataset
# MAGIC

# COMMAND ----------

dbutils.fs.mkdirs('dbfs:/FileStore/udemy_mount')

# COMMAND ----------



dbutils.fs.mv('file:/udemy/','dbfs:/FileStore/udemy_mount',recurse=True)


# COMMAND ----------

kaggle_path='udemy'

file_path='dbfs:/dbfs/FileStore/udemy_mnt'


# COMMAND ----------

# import zipfile
# with zipfile.ZipFile('/databricks/driver/2020-udemy-courses-dataset.zip')as zp:
#     zp.extractall('/{}/'.format(kaggle_path))
#     dbutils.fs.mv('file://',file_path+'{}'.format(kaggle_path),True)
#     dbutils.fs.rm('/databricks/driver/ipl-2017.zip')

# COMMAND ----------

pip install beautifulsoup4

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/udemy_mount/'))

# COMMAND ----------

from pyspark.sql.functions import lit
# df_tech_raw=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mnt/udemy_tech.csv")
df_business_raw=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mount/udemy_business.csv")


df_tech_raw=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mount/udemy_tech.csv")
# df_business_raw=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mount/udemy_business.csv")

# df_tech=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mount/udemy_finance.csv")
df_finance_raw=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mount/udemy_finance.csv")

df_office_raw=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mount/udemy_office_productivity.csv")
# df_finance_raw=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mount/udemy_finance.csv")
df_life_style_raw=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mount/udemy_lifestyle.csv")
df_music_raw=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mount/udemy_music.csv")


df_udemy_design_raw=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mount/udemy_design.csv")
df_marketing_raw=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mount/udemy_marketing.csv")
df_photography_raw=spark.read.option('header',True).csv("dbfs:/FileStore/udemy_mount/udemy_photography.csv")






# df2 = df1.limit(40)




# COMMAND ----------

# adding extra  key column
df_tech_raw=df_tech_raw.withColumn('category',lit('tech'))
df_business_raw=df_business_raw.withColumn('category',lit('bussiness'))
df_finance_raw=df_finance_raw.withColumn('category',lit('finance'))
df_office_raw=df_office_raw.withColumn('category',lit('office'))
df_life_style_raw=df_life_style_raw.withColumn('category',lit('life_style'))
df_music_raw=df_music_raw.withColumn('category',lit('music'))
df_udemy_design_raw=df_udemy_design_raw.withColumn('category',lit('design'))
df_marketing_raw=df_marketing_raw.withColumn('category',lit('marketing'))
df_photography_raw=df_photography_raw.withColumn('category',lit('photography'))



# COMMAND ----------


df_tech_raw=df_tech_raw.limit(40)

# COMMAND ----------



df_business_raw=df_business_raw.limit(40)
df_finance_raw=df_finance_raw.limit(40)
df_office_raw=df_office_raw.limit(40)
df_life_style_raw=df_life_style_raw.limit(40)
df_music_raw=df_music_raw.limit(40)
df_udemy_design_raw=df_udemy_design_raw.limit(40)
df_marketing_raw=df_marketing_raw.limit(40)
df_photography_raw=df_photography_raw.limit(40)

# COMMAND ----------

# MAGIC %md #union of multiple DataFrames

# COMMAND ----------

# src_final_df=df_tech_raw.union(df_business_raw)



src_final_df = df_business_raw.union(df_finance_raw).union(df_office_raw).union(df_life_style_raw).union(df_music_raw).union(df_udemy_design_raw).union(df_marketing_raw).union(df_photography_raw).union(df_tech_raw)

# COMMAND ----------

src_final_df.count()

# COMMAND ----------

display(src_final_df)

# COMMAND ----------

# dbutils.fs.rm("dbfs:/mini_project/DataLake/bronze/udemy1", True)
src_final_df.write.format("parquet").mode("overwrite").saveAsTable("bronze.udemy1")



# COMMAND ----------

spark.sql("REFRESH TABLE bronze.udemy1")
