# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 18:22:03 2020

@author: Jimi
"""

import os
import json
from pymongo import MongoClient


data_path = './sorted_data'

reviews = []

for root, dirs, files in os.walk(data_path):
    root = root.replace('\\', '/')
    root_split = root.split('/')
    if len(root_split) > 2:
    # print(files)
        
        cat = root_split[2]
        if cat in ['books', 'dvd', 'music']:
            continue
        
        if files[0] == 'all.review':   
            path = root+'/'+files[0]
            
            
            print('Working on', path)
            review = {}
            with open(path, encoding='cp1252') as f:
                lines = f.readlines()
                for line in lines:
                    if line[0] == '<':
                        head = line[1:-2]
                        if head[0] == '/':
                            tail = head[1:]
                            if tail == 'review':
                                reviews.append(review)
                        elif head == 'review':
                            review = {}
                        else:
                            count = 0
                            while head in review.keys():
                                count += 1
                                head = head+'_'+str(count)
                            review[head] = ''
                    else:
                        review[head] += line.strip().replace('\\', '')
                    
                    
outfile = path = data_path+'/all.filt'
with open(outfile, 'w') as fout:
    json.dump(reviews, fout)
            
print('Connecting to MongoDB')
client = MongoClient('mongodb://localhost:27017')
db = client['reviews']

if 'all' in db.list_collection_names():
    db['all'].drop()

collection = db['all']

print('Inserting values into reviews database under collection "all"')
collection.insert_many(reviews)
