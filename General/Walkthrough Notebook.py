# Databricks notebook source
# MAGIC %md
# MAGIC ## Magic Commands
# MAGIC Databricks contains several different magic commands.
# MAGIC
# MAGIC #### Mix Languages
# MAGIC You can override the default language by specifying the language magic command `%<language>` at the beginning of a cell. The supported magic commands are: 
# MAGIC * `%python`
# MAGIC * `%r`
# MAGIC * `%scala`
# MAGIC * `%sql`
# MAGIC
# MAGIC #### Auxiliary Magic Commands
# MAGIC * `%sh`: Allows you to run shell code in your notebook. To fail the cell if the shell command has a non-zero exit status, add the -e option. This command runs only on the Apache Spark driver, and not the workers. 
# MAGIC * `%fs`: Allows you to use dbutils filesystem commands.
# MAGIC * `%md`: Allows you to include various types of documentation, including text, images, and mathematical formulas and equations.
# MAGIC
# MAGIC #### Other Magic Commands
# MAGIC * `%tensorboard`: starts a TensorBoard server and embeds the TensorBoard user interface inside the Databricks notebook for data scientists and machine learning engineers to visualize and debug their machine learning projects. More info [here](https://databricks.com/blog/2020/08/25/tensorboard-a-new-way-to-use-tensorboard-on-databricks.html)
# MAGIC * `%pip`: Allows you to easily customize and manage your Python packages on your cluster i,e: %pip install databricks-sdk
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Tables/Files

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bolt_dcp_int.gold.watch_time_by_video_session

# COMMAND ----------

df_wtbvs = spark.read.table("bolt_dcp_int.gold.watch_time_by_video_session")

# COMMAND ----------

# MAGIC %python
# MAGIC df_airlines = spark.read.option("header",True).csv("/databricks-datasets/asa/airlines/2008.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Transformations
# MAGIC

# COMMAND ----------

display(df_wtbvs.groupBy('platform').count())

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

filter_df = df_wtbvs.filter(f.col('platform') == 'firetv')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing to Unity Catalog
# MAGIC

# COMMAND ----------

display(filter_df)

# python
# filter_df.write.saveAsTable("bolt_dcp_int.gold.filter_watch_time_by_video_session")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- CREATE TABLE bolt_dcp_int.gold.filter_watch_time_by_video_session
# MAGIC -- AS SELECT * FROM bolt_dcp_int.gold.watch_time_by_video_session WHERE platform = 'firetv'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pandas
# MAGIC
# MAGIC We’re thrilled to announce that the pandas API will be part of the upcoming Apache Spark™ 3.2 release. pandas is a powerful, flexible library and has grown rapidly to become one of the standard data science libraries. Now pandas users will be able to leverage the pandas API on their existing Spark clusters.

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC
# MAGIC pd_wtbvs = df_wtbvs.toPandas()

# COMMAND ----------

pd_wtbvs.head(15)

# COMMAND ----------


