# Databricks notebook source
# MAGIC %run "include/env"

# COMMAND ----------

# Imports
import pandas as pd
import time
from pyspark.sql.functions import countDistinct

path = _storage + "/users/mw/dms_analysis/"

# COMMAND ----------

fields_file_path = path + "BDL fields.csv"
fields_df = spark.read.csv(fields_file_path, header=True)

# COMMAND ----------

xlsx_file_path = path + "BDL fields.xlsx"
pd.read_excel(xlsx_file_path)

# COMMAND ----------

def sort_unique_values(df, field_name):
    results = sorted(
      df.groupBy(field_name).agg(countDistinct('FIGI')).collect(),
      key=lambda x: x[1], reverse=True
    )
    spark.createDataFrame(results).display()

# COMMAND ----------

field_name = "REG_S_TRAD_REST_LST_ADDL_OFFER"
sort_unique_values(fields_df, field_name)

# COMMAND ----------

field_name = "ROLLING_SERIES"
sort_unique_values(fields_df, field_name)

fields_df.filter(col(field_name).isin([' ', 'N.A.']) == False).count()

# COMMAND ----------

sorted(fields_df.columns)

# COMMAND ----------

# full pil
pil = tables.get.pil.portfolio_lookthrough_ibr().current()

# filter to latest
latest_date = pil.agg({"valueDate": "max"}).collect()[0][0]
pil = pil.filter(col("valueDate")==latest_date"2022-11-08")

# COMMAND ----------

pil.groupBy("instrumentGroup").agg(countDistinct("instrumentId")).display()

# COMMAND ----------

pil.groupBy(["portfolioCode", "instrumentGroup"]).agg(countDistinct("instrumentId")).display()