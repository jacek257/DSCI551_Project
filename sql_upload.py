import mysql.connector
import os
import sys
import numpy as np
import pandas as pd

data_path = './sorted_data'

# db = mysql.connector.connect(host='localhost', user='root', password='root')
# print('Connected to mysql:', db.is_connected())

# cursor = db.cursor()

# cursor.execute("CREATE DATABASE IF NOT EXISTS review_sentiment")
# cursor.close()

# db = mysql.connector.connect(host='localhost', user='root', password='root', database='review_sentiment')
# print('Connected to database:', db.is_connected())

# cursor = db.cursor()
# cursor.execute('CREATE TABLE IF NOT EXISTS reviews (id VARCHAR(255), p_name VARCHAR(255), p_type VARCHAR(255), rating FLOAT, text LONGTEXT)')
# cursor.close()

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
        
        if done:
            print(cat, 'has clean file')
            continue
            
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

#         print('Inserting into mysql')
#         query = 'INSERT INTO review.reviews(id, p_name, p_type, rating, text) VALUES (%s, %s, %s, %s, %s)'
        
#         cursor = db.cursor()
#         sql_tuples_no_dup = list(set([i for i in sql_tuples]))
#         cursor.executemany(query, sql_tuples_no_dup)
#         cursor.close()
#         print('Committing')
#         db.commit()
#         print(cursor.rowcount, 'inserts done')

# print('reviews database populated')

    
#         # for t in dom.iter('review'):
#         #     print(t)