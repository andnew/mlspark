#!/bin/python
# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import numpy as np

spark = SparkSession.builder.appName("movie_list").getOrCreate()

spark.read.csv(sep="|", path="/Users/wangyong/xcript/ml/ml-100k/u.item") \
    .selectExpr("_c0 as id", "_c1 as name", "_c2 as yr", "_c4 as url") \
    .createOrReplaceTempView("movie")

"""
 采用的条状图进行数据输出

df_years = spark.sql("""
# select
#   age ,
#   count(1) as cnt
# from
# (
#     select
#       1998 - cast(if(length(yr) > 0,substring(yr,8,12),1900) as int) as age
#     from movie
#     where length(yr) >0
# )m
# group by age
""").cache()

movies_ages = df_years.rdd.map(lambda row: (row['age'], row['cnt'])).collect()

x_axis = np.array([c[0] for c in movies_ages])
y_axis = np.array([c[1] for c in movies_ages])

pos = np.arange(len(x_axis))
width = 1.0

ax = plt.axes()
ax.set_xticks(pos + (width / 2))
ax.set_xticklabels(x_axis)

plt.bar(pos, y_axis, width, color='lightblue')
plt.xticks(rotation=30)
fig1 = plt.gcf()
fig1.set_size_inches(16, 10)
plt.grid(True)
plt.show()

"""

"""
书中的源代码
"""


def convert_year(x):
    try:
        return int(x[-4:])
    except:
        return 1900  # 年份缺失的情况


years_filtered = spark.sql("""
select 
     yr 
from movie
""").rdd.map(lambda row: convert_year(row['yr'])).filter(lambda x: x != 1900)

movie_ages = years_filtered.map(lambda yr: 1998 - yr).countByValue()

values = movie_ages.values()
bins = movie_ages.keys()

plt.hist(values, bins=bins, color='lightblue', normed=True)
# plt.hist(movie_ages, color='lightblue', normed=True)
fig = plt.gcf()
fig.set_size_inches(10, 7)
plt.show()