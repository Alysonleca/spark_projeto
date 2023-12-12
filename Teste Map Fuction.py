# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

conf = SparkConf().setAppName("Teste Map")

# COMMAND ----------

sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Teste_Map.txt')
rdd.collect()

# COMMAND ----------

def contar(x):
    l = x.split(' ')
    l2 = []
    for i in l:
        l2.append(len(i))
    return l2

# COMMAND ----------

rdd2 = rdd.map(contar)
rdd2.collect()
