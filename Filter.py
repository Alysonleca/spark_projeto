# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Flat_Map")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Sample.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.filter(lambda x: x != '12 12 33')
rdd2.collect()


# COMMAND ----------

r
