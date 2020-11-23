# -*- coding: utf-8 -*-
"""
Created on Sun Nov 22 15:30:39 2020

@author: Jimi
"""

import pymongo
from bson.json_util import dumps
from pyspark.sql import SparkSession
from pyspark.sql import functions as fxn
import mysql.connector 
import numpy as np

def _get_rdd(keyword, spark):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['reviews']
    col = db['all']

    doc = list(col.find({'review_text' : {'$regex' : '\\b'+keyword+'\\b', '$options' : 'i'}}, {'_id' : 0, 'product_type_1' : 0, 'unique_id' : 0, 'unique_id_1' :0}))
    
    return spark.sparkContext.parallelize(doc).map(lambda x: dumps(x, indent=0))

def get_review(keyword, category=None):

    spark = SparkSession.builder.appName('sentiment').getOrCreate()
    
    rdd = _get_rdd(keyword, spark)
    df = spark.read.option("multiline", "true").json(rdd).sort('product_name')
    
    df2 = df.selectExpr('product_type', 'cast(rating as float) rating')
    avg_ratings = df2.groupBy('product_type').agg(fxn.round(fxn.mean('rating').alias('avg_rating'),2)).sort('product_type').collect()
    ratings = []
    for item in avg_ratings:
        rating = []
        for entity in item:
            rating.append(entity)
        ratings.append(rating)
    
    ratings.append(['All', round(np.mean(np.array(ratings)[:,1].astype(np.float)),2)])
    
    if category:
        df = df.filter(df.product_type == category)
    
    reviews = set()
    for item in df.collect():
        review = []
        for entity in item:
            review.append(entity)
        reviews.add(tuple(review))
    
    return reviews, ratings

def get_sentiment( keyword):
    error = False
    
    db = mysql.connector.connect(host='localhost', user='root', password='root', database='review_sentiment')
    cursor = db.cursor()
    
    cursor.execute("SHOW TABLES")
    
    tables = cursor.fetchall()
    
    output = []
    keyword = keyword.replace(' ','_')
    for table in tables:
        table = table[0]
        # print(table)
        cursor.execute("SELECT positive, negative FROM `"+table+'` WHERE keyword="'+keyword+'";')
        out = cursor.fetchone()
        if out:

            output.append([table] + list(out))
    
    if len(output):
        output.append(['All', sum(np.array(output)[:,1].astype(int)), sum(np.array(output)[:,2].astype(int))])
    else:
        error = True
    
    return output, error

def get_info(keyword, category=None):
    sentiment, error = get_sentiment(keyword)
    
    if error:
        return None, None, error
    
    reviews, ratings = get_review(keyword, category)
    
    for i in range(len(ratings)):
        for j in range(len(sentiment)):
            if ratings[i][0].replace(' ','_').replace('amp;','') == sentiment[j][0]:
                sentiment[j].append(ratings[i][1])
                break
    
    for i in range(len(sentiment)):
        if len(sentiment[i]) < 4:
            sentiment[i].append('')
    
    
    
    return reviews, sentiment, error