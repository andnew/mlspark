#!/bin/python
# -*- coding:utf-8 -*-

from pyspark import SparkContext

sc = SparkContext("local[4]", "First Spark App")
data = sc.textFile("/Users/wangyong/xcript/ml/ml-100k/u.data")\
    .map(lambda line: line.split("\t"))\
    .map(lambda record: (record[0], record[1], record[2], record[3]))

print(data.take(2))


