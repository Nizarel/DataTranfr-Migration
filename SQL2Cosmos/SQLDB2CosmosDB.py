# Databricks notebook source
# MAGIC %md
# MAGIC ##Migrating Relational Data with one-to-few relationships into Cosmos DB SQL API
# MAGIC
# MAGIC Here we are considering a simple order system where each order can have multiple detail lines. In this scenario, the relationship is not unbounded, and there is a limited number of detail lines that may exist for a given order. We can consider this a one-to-few relationship. This is a good candidate for denormalizion. Typically denormalized data models provide better read performance in distributed databases, since we will minimise the need to read across data partitions.

# COMMAND ----------

display(dbutils.secrets.listScopes())
dbutils.secrets.list(scope="lakevault-secret")

# COMMAND ----------

import uuid
import json
import ast
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType,DateType,LongType,IntegerType,TimestampType
from multiprocessing.pool import ThreadPool

#JDBC connect details for SQL Server database
jdbcHostname = "tjxserv.database.windows.net"
jdbcDatabase = "tjxsalesdb"
jdbcUsername = "tjxadmin"
jdbcPort = "1433"


cosmosEndpoint = "https://tjx.documents.azure.com:443/"
cosmosDatabaseName = "SalesDb"
cosmosContainerName = "Orders"

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : dbutils.secrets.get(scope = "lakevault-secret", key = "CosmosKey"),
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
}

#get all orders
orders = (spark.read
  .format("sqlserver")
  .option("host", jdbcHostname)
  .option("user", jdbcUsername )
  .option("password", dbutils.secrets.get(scope = 'lakevault-secret', key = 'sqlpwd'))
  .option("database", jdbcDatabase)
  .option("dbtable", "SalesLT.SalesOrderHeader") # (if schemaName not provided, default to "dbo")
  .load()
)

#get all order details
orderdetails = (spark.read
  .format("sqlserver")
  .option("host", jdbcHostname)
  .option("user", jdbcUsername )
  .option("password", dbutils.secrets.get(scope = 'lakevault-secret', key = 'sqlpwd'))
  .option("database", jdbcDatabase)
  .option("dbtable", "SalesLT.SalesOrderDetail") # (if schemaName not provided, default to "dbo")
  .load()
)

#get all OrderId values to pass to map function 
orderids = orders.select('SalesOrderID').collect()

#create thread pool big enough to process merge of details to orders in parallel
pool = ThreadPool(10)


# COMMAND ----------

def writeOrder(orderid):
  #order = orders.filter(col('SalesOrderID').isin(orderid))  
  order = orders.filter(orders['SalesOrderID'] == orderid[0])
  #set id to be a uuid
  order = order.withColumn("id", lit(str(uuid.uuid1())))
  
  #add details field to order dataframe
  order = order.withColumn("details", lit(''))
  
  #filter order details dataframe to get details we want to merge into the order document
  orderdetailsgroup = orderdetails.filter(orderdetails['SalesOrderID'] == orderid[0])
  
  #convert dataframe to pandas
  orderpandas = order.toPandas()
  
  #convert the order dataframe to json and remove enclosing brackets
  orderjson = orderpandas.to_json(orient='records', force_ascii=False)
  orderjson = orderjson[1:-1] 
  
  #convert orderjson to a dictionaory so we can set the details element with order details later
  orderjsondata = json.loads(orderjson)
  
  #convert orderdetailsgroup dataframe to json, but only if details were returned from the earlier filter
  if (orderdetailsgroup.count() !=0):
    #convert orderdetailsgroup to pandas dataframe to work better with json
    orderdetailsgroup = orderdetailsgroup.toPandas()
    
    #convert orderdetailsgroup to json string
    jsonstring = orderdetailsgroup.to_json(orient='records', force_ascii=False)
    
    #convert jsonstring to dictionary to ensure correct encoding and no corrupt records
    jsonstring = json.loads(jsonstring)
    
    #set details json element in orderjsondata to jsonstring which contains orderdetailsgroup - this merges order details into the order 
    orderjsondata['details'] = jsonstring
    
  #convert dictionary to json
  orderjsondata = json.dumps(orderjsondata)
  
  #read the json into spark dataframe
  df = spark.read.json(sc.parallelize([orderjsondata]))

    #write the dataframe (this will be a single order record with merged many-to-one order details) to cosmos db using spark the connector
  df.write.format("cosmos.oltp").mode("append").options(**cfg).save()



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Create a function for writing Orders into the target SQL API collection. This function will filter all order details for the given order id, convert them into a JSON array, and insert the array into a JSON document that we will write into the target SQL API Collection for that order:
# MAGIC

# COMMAND ----------

#map order details to orders in parallel using the above function
pool.map(writeOrder, orderids)
