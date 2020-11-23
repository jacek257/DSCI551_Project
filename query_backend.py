# -*- coding: utf-8 -*-
"""
Created on Sun Nov 22 15:30:39 2020

@author: Jimi
"""

import pymongo
from bson.json_util import dumps
from pyspark.sql import SparkSession
import mysql.connector 
import numpy as np

class request:
    def _get_rdd(self, keyword, spark):
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client['reviews']
        col = db['all']
    
        doc = list(col.find({'review_text' : {'$regex' : '\\b'+keyword+'\\b', '$options' : 'i'}}))
        
        return spark.sparkContext.parallelize(doc).map(lambda x: dumps(x, indent=0))
    
    def _get_review(self, keyword, category=None):
    
        spark = SparkSession.builder.appName('sentiment').getOrCreate()
        
        rdd = self._get_rdd(keyword, spark)
        df = spark.read.option("multiline", "true").json(rdd)
        if category:
            df = df.filter(df.product_type == category)
        return df.collect()
    
    def _get_sentiment(self, keyword):
        db = mysql.connector.connect(host='localhost', user='root', password='root', database='review_sentiment')
        cursor = db.cursor()
        
        cursor.execute("SHOW TABLES")
        
        tables = cursor.fetchall()
        
        output = []
        for table in tables:
            table = table[0]
            # print(table)
            cursor.execute("SELECT * FROM `"+table+'` WHERE keyword="'+keyword+'";')
            out = cursor.fetchone()
            if out:
                output.append([table] + list(out))
        
        output.append(['Total', sum(np.array(output)[:,2].astype(int)), sum(np.array(output)[:,3].astype(int))])
        
        return output
    
    def get_info(self, keyword, category=None):
        reviews = self._get_review(keyword, category)
        sentiment = self._get_sentiment(keyword)
        
        return reviews, sentiment