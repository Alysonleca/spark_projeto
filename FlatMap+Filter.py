# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

conf = SparkConf().setAppName("Quiz2")


# COMMAND ----------

sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/Quiz2.txt')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.map(lambda x: x.split(' '))
rdd2.collect()

# COMMAND ----------

def inicial(x):
    l = x.split(' ')
    l2 = []
    for i in l:
        l2.append(i[0])
    return l2
rdd3 = rdd.flatMap(inicial)
rdd3.collect()

# COMMAND ----------


rdd4 = rdd3.filter(lambda x: x != 'a' and x != 'c')
rdd4.collect()

# COMMAND ----------


