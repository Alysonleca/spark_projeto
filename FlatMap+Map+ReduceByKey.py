# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

conf = SparkConf().setAppName('QuizCount')

# COMMAND ----------

sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

QuizBase = sc.textFile('/FileStore/tables/count.txt')

# COMMAND ----------

QuizBase.collect()

# COMMAND ----------

rdd = QuizBase.flatMap(lambda x: x.split(' '))
rdd.collect()

# COMMAND ----------

rdd1 = rdd.map(lambda x: (x, len(x)))
rdd1.collect()

# COMMAND ----------

rdd2 = rdd1.reduceByKey(lambda x,y: x + y)
rdd2.collect()

# COMMAND ----------


