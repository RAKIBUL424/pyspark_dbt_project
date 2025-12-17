# Databricks notebook source
# MAGIC %md
# MAGIC ### **CUSTOMERS**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import os
import sys
current_dir = os.getcwd()
sys.path.append(current_dir)

# COMMAND ----------

# from typing import List
# from pyspark.sql import DataFrame
# from pyspark.sql.window import Window
# from pyspark.sql.functions import current_timestamp



# class Transformation:
#     def dedup(self, df: DataFrame, dedup_cols: List, cdc: str):

#         df = df.withColumn("deduoKey", concat(*dedup_cols))
#         df = df.withColumn("dedupCounts", row_number(). over(Window.partitionBy("deduoKey").orderBy(cdc)))
#         df = df.filter(df.dedupCounts == 1)
#         df = df.drop("dedup", "dedupCounts")

#         return df
    
#     def process_timestamp(self, df):

#         df = df.withColumn("process_timestamp", current_timestamp())
#         return df
    
#     def upsert(self, df, key_cols, table,cdc):
#         merge_condition = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])
        
#         dlt_obj = DeltaTable.forName(spark,f"pysparkdbt.silver.{table}")
#         dlt_obj.alias("trg").merge(
#             df.alias("src"),
#             merge_condition)\
#                 .whenMatchedUpdateAll(condition= f"src.{cdc} >= trg.{cdc}")\
#                 .whenNotMatchedInsertAll()\
#                 .execute()
#         return df


# COMMAND ----------



# COMMAND ----------

# df_cust = df_cust.withColumn("email", split(df_cust.email, "@")[1])

# df_cust = df_cust.withColumn("phone_number", regexp_replace(df_cust.phone_number, r"[^0-9]",""))
# display(df_cust)

# df_cust = df_cust.withColumn("full_name", concat_ws(" ", df_cust.first_name, df_cust.last_name))

# df_cust = df_cust.drop('first_name','last_name','phone')


# COMMAND ----------

# class BaseLineProcessing():
#     def email_processing(self, df: DataFrame, col: List):
#         df = df.withColumn("col", split(df.col, "@")[1])

# COMMAND ----------



# COMMAND ----------

df_cust = spark.read.table("pysparkdbt.bronze.customers")


# COMMAND ----------

df_cust = df_cust.withColumn("email", split(df_cust.email, "@")[1])
display(df_cust)

# COMMAND ----------

df_cust = df_cust.withColumn("phone_number", regexp_replace(df_cust.phone_number, r"[^0-9]",""))


# COMMAND ----------

df_cust = df_cust.withColumn("full_name", concat_ws(" ", df_cust.first_name, df_cust.last_name))


# COMMAND ----------

df_cust = df_cust.drop('first_name','last_name','phone')

# COMMAND ----------

display(df_cust)

# COMMAND ----------

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import current_timestamp



class Transformation:
    def dedup(self, df: DataFrame, dedup_cols: List, cdc: str):

        df = df.withColumn("deduoKey", concat(*dedup_cols))
        df = df.withColumn("dedupCounts", row_number(). over(Window.partitionBy("deduoKey").orderBy(cdc)))
        df = df.filter(df.dedupCounts == 1)
        df = df.drop("dedup", "dedupCounts")

        return df
    
    def process_timestamp(self, df):

        df = df.withColumn("process_timestamp", current_timestamp())
        return df
    
    def upsert(self, df, key_cols, table,cdc):
        merge_condition = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])
        
        dlt_obj = DeltaTable.forName(spark,f"pysparkdbt.silver.{table}")
        dlt_obj.alias("trg").merge(
            df.alias("src"),
            merge_condition)\
                .whenMatchedUpdateAll(condition= f"src.{cdc} >= trg.{cdc}")\
                .whenNotMatchedInsertAll()\
                .execute()
        return df


# COMMAND ----------

cust_obj = Transformation()
cust_df_trans = cust_obj.dedup(df_cust, ['customer_id'], 'last_updated_timestamp')
cust_df_trans = cust_obj.process_timestamp(cust_df_trans)
display(cust_df_trans)

# COMMAND ----------

from delta.tables import DeltaTable

if not spark.catalog.tableExists("pysparkdbt.silver.customers"):
    df_cust.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.customers")
    
else:
    delta_table = DeltaTable.forName(
        spark,
        "pysparkdbt.silver.customers"
    )
    delta_table.alias("t").merge(
        source=df_cust.alias("s"),
        condition="t.customer_id = s.customer_id"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

# COMMAND ----------

df_driver= spark.read.table("pysparkdbt.bronze.drivers")
display(df_driver)

# COMMAND ----------

df_driver.columns

# COMMAND ----------

# df_drive = df_driver.withColumn("email", split(df_driver.email, "@")[1])

df_driver = df_driver.withColumn("phone_number", regexp_replace(df_driver.phone_number, r"[^0-9]",""))


df_driver = df_driver.withColumn("full_name", concat_ws(" ", df_driver.first_name, df_driver.last_name))

df_driver = df_driver.drop('first_name','last_name')


# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

target_table = "pysparkdbt.silver.drivers"

if not spark.catalog.tableExists(target_table):
    driver_df_trans.write.format("delta") \
        .mode("append") \
        .saveAsTable(target_table)
else:
    target_cols = [f.name for f in spark.table(target_table).schema.fields]
    for col_name in target_cols:
        if col_name not in driver_df_trans.columns:
            driver_df_trans = driver_df_trans.withColumn(col_name, F.lit(None))
    driver_df_trans = driver_df_trans.select(target_cols)
    delta_table = DeltaTable.forName(spark, target_table)
    delta_table.alias("t").merge(
        source=driver_df_trans.alias("s"),
        condition="t.driver_id = s.driver_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### LOCATIONS

# COMMAND ----------

df_locations= spark.read.table("pysparkdbt.bronze.locations")
display(df_locations)

# COMMAND ----------

loc_obj = Transformation()
loc_df_trans = loc_obj.dedup(df_locations, ['location_id'], 'last_updated_timestamp')
loc_df_trans = loc_obj.process_timestamp(loc_df_trans)

# COMMAND ----------

if not spark.catalog.tableExists("pysparkdbt.silver.locations"):
    loc_df_trans.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.locations")
else:
    loc_df_trans.upsert(loc_df_trans, ['location_id'], 'locations', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **PAYMENTS**

# COMMAND ----------

df_pay = spark.read.table("pysparkdbt.bronze.payments")
display(df_pay)

# COMMAND ----------

from pyspark.sql.functions import when

df_pay = df_pay.withColumn(
    "online_payment_status",
    when(
        (df_pay.payment_method == "Card") & (df_pay.payment_status == "Success"),
        "online-success"
    ).when(
        (df_pay.payment_method == "Card") & (df_pay.payment_status == "Failed"),
        "online-failed"
    ).when(
        (df_pay.payment_method == "Cash") & (df_pay.payment_status == "Pending"),
        "online-pending"
    ).otherwise("offline")
)
display(df_pay)

# COMMAND ----------

from delta.tables import DeltaTable
payment_obj = Transformation()

df_pay_trans = payment_obj.dedup(df_pay, ['payment_id'], 'transaction_time')
df_pay_trans = payment_obj.process_timestamp(df_pay_trans)
if not spark.catalog.tableExists("pysparkdbt.silver.payments"):
    df_pay_trans.write.format("delta").mode("append").saveAsTable("pysparkdbt.silver.payments")
else:
    payment_obj.upsert(df_pay_trans, ['payment_id'], 'payments', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.silver.payments

# COMMAND ----------

# MAGIC %md
# MAGIC ### **VEHICLES**

# COMMAND ----------

df_vehicles = spark.read.table("pysparkdbt.bronze.vehicles")
display(df_vehicles)

# COMMAND ----------

df_vehicles = df_vehicles.withColumn("make", upper(df_vehicles.make))
display(df_vehicles)

# COMMAND ----------

vehicles_obj = Transformation()
df_vehicles_trans = vehicles_obj.dedup(df_vehicles, ['vehicle_id'], 'last_updated_timestamp')
df_vehicles_trans = vehicles_obj.process_timestamp(df_vehicles_trans)
if not spark.catalog.tableExists("pysparkdbt.silver.vehicles"):
    df_vehicles_trans.write.format("delta").mode("append").saveAsTable("pysparkdbt.silver.vehicles")
else:
    vehicles_obj.upsert(df_vehicles_trans, ['vehicle_id'], 'vehicles', 'last_updated_timestamp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from pysparkdbt.silver.vehicles

# COMMAND ----------

