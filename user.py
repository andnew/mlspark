#!/bin/python
# -*- coding:utf-8 -*-

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import numpy as np

spark = SparkSession.builder.appName("movie_list").getOrCreate()

spark.read.csv(sep="|", path="/Users/wangyong/xcript/ml/ml-100k/u.user") \
    .selectExpr("_c0 as id", "_c1 as age", "_c2 as gender", "_c3 as occupation", "_c4 as zip_code") \
    .createOrReplaceTempView("user")

spark.sql("""
select 
 count(1) as num_user,
 count(distinct gender) as num_genders,
 count(distinct occupation) as num_occupations,
 count(distinct zip_code) as num_zip_codes
from user
""").show()

df_user = spark.sql("""
select 
  age 
from user
""").cache()

# 年龄直方图
# ages = df_user.select("age").rdd.map(lambda row: int(row['age'])).collect()
# plt.hist(ages, bins=20, color='lightblue', normed=True)
# fig = plt.gcf()
# fig.set_size_inches(5, 4)
# plt.grid(True)
# plt.show()

# 职业的bar图
count_by_occupation = spark.sql("""
select 
  occupation,
  count(1) as cnt
from user
group by occupation
order by cnt 
""").rdd.map(lambda row: (row['occupation'], row['cnt'])).collect()

# x_axis1 = np.array([c[0] for c in count_by_occupation])
# y_axis1 = np.array([c[1] for c in count_by_occupation])
x_axis = np.array([c[0] for c in count_by_occupation])
y_axis = np.array([c[1] for c in count_by_occupation])
#
# x_axis = x_axis1[np.argsort(y_axis1)]
# y_axis = y_axis1[np.argsort(y_axis1)]

pos = np.arange(len(x_axis))
width = 1.0

ax = plt.axes()
ax.set_xticks(pos + (width / 2))
ax.set_xticklabels(x_axis)

plt.bar(pos, y_axis, width, color='lightblue')
plt.xticks(rotation=30)
fig1 = plt.gcf()
fig1.set_size_inches(5, 4)
plt.grid(True)
plt.show()

# countByValue 统计各个不同值所分别出现的次数
count_by_occupation2 = spark.sql("""
select occupation from user
""").rdd.map(lambda row: row['occupation']).countByValue()
print("Map-reduce approach:")
opc2 = dict(count_by_occupation2)
sorted(opc2.items(), key=lambda x: int(x[1]), reverse=False)
print(opc2)
print("")
print("countByValue approach")
print(dict(count_by_occupation))
