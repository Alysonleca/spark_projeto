# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Flat_Map")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Sample.txt')
rdd.collect()

# COMMAND ----------

mappedrdd = rdd.map(lambda x: x.split(" "))
mappedrdd.collect()

# COMMAND ----------

flatmappedrdd = rdd.flatMap(lambda x: x.split(" "))
flatmappedrdd.collect()
