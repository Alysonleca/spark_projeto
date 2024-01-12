# Databricks notebook source
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Average")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd = sc.textFile('/FileStore/tables/average_quiz_sample.csv')
rdd.collect()

# COMMAND ----------

rdd2 = rdd.map(lambda x: (x.split(',')[1], (float(x.split(',')[2]))))
rdd3 = rdd2.reduceByKey(lambda x,y: x if x < y else y)
rdd3.collect()

# COMMAND ----------

rdd4 = rdd.map(lambda x: (x.split(',')[1], (int(x.split(',')[2]))))
rdd5 = rdd2.reduceByKey(lambda x,y: x if x > y else y)
rdd5.collect()

# COMMAND ----------



# COMMAND ----------


