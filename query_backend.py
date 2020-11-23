# -*- coding: utf-8 -*-
"""
Created on Sun Nov 22 15:30:39 2020

@author: Jimi
"""

import pymongo
from bson.json_util import dumps
from pyspark.sql import SparkSession
import mysql.connector 

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['reviews']
col = db['all']

keyword = 'sued'
product = 'apparel'

doc = list(col.find({'review_text' : {'$regex' : '\\b'+keyword+'\\b', '$options' : 'i'}}))

json_doc = dumps(doc)

spark = SparkSession.builder.appName('sentiment').getOrCreate()
rdd = spark.sparkContext.parallelize(doc).map(lambda x: dumps(x, indent=0))
# rddCollect = rdd.collect()
# print(rddCollect)
# print(rdd)

df = spark.read.option("multiline", "true").json(rdd)
filt = df.filter(df.product_type == product).select('product_type').collect()
print(filt)

db = mysql.connector.connect(host='localhost', user='root', password='root', database='review_sentiment')
cursor = db.cursor()

cursor.execute("SHOW TABLES")

tables = cursor.fetchall()

pos = []
neg = []
for table in tables:
    table = table[0]
    print(table)
    cursor.execute("SELECT * FROM `"+table+'` WHERE keyword="'+keyword+'";')
    out = cursor.fetchone()
    print(out)
    if out:
        pos.append(out[1])
        neg.append(out[2])

print('Total sentiment for', keyword, 'is', str(sum(pos)), 'positive and', str(sum(neg)), 'negative')