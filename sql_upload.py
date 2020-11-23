import mysql.connector
import os
import sys
import pandas as pd
from sqlalchemy import create_engine

data_path = './sorted_data'


engine = create_engine('mysql+mysqlconnector://root:root@localhost')
engine.execute("CREATE DATABASE IF NOT EXISTS review_sentiment")
engine.execute("USE review_sentiment")

print('Grabbing relevant data')
for root, dirs, files in os.walk(data_path):
    root = root.replace('\\', '/')
    root_split = root.split('/')
    if len(root_split) > 2:
    # print(files)
        
        cat = root_split[2]
        if cat in ['books', 'dvd', 'music']:
            continue
        
        path = ''
        done = False
        for file in files:
            if file == 'processed.review':
                path = root+'/'+file
            if file == 'processed.review.clean':
                done = True
        
        if not done:            
            if path == '':
                print("No processed.review file")
                sys.exit()
            
            print('Working on', path)
            
            keywords = pd.DataFrame(columns=['keyword', 'positive', 'negative'])
            keys = set()
            
            with open(path, encoding='cp1252') as f:
                lines = f.readlines()
                line_count = len(lines)
                count = 0
                for line in lines:
                    count += 1
                    line = line.strip().split(' ')
                    features = line[:-1]
                    label = line[-1].split(':')[-1]
                    for feature in features:
                        feature = feature.split(':')
                        feat = feature[0]
                        
                        pos = 0
                        neg = 0
                        if label == 'positive':
                            pos = int(feature[1])
                        else:
                            neg = int(feature[1])
                            
                        length = len(keys)
                        keys.add(feat)
                            
                        if keywords.empty or (length != len(keys)):
                            # print(feat)
                            keywords = keywords.append({'keyword':feat, 'positive':pos, 'negative':neg}, ignore_index=True)
                        else:
                            keywords.loc[keywords['keyword'] == feat, 'positive'] += pos
                            keywords.loc[keywords['keyword'] == feat, 'negative'] += neg
                    print(cat, count, 'of', line_count, 'done')
                    
            print("Writing to file")
            keywords.to_csv(path+'.clean', index=False)
        else:
            print(cat, 'has clean file')
        
        df = pd.read_csv(path+'.clean')
        try:
            df.to_sql(name=cat, con=engine, if_exists='fail', index=False, chunksize=1000)
        except:
            print('SQL upload failed for '+cat+': Check if already exists')

print('review sentiment database populated')