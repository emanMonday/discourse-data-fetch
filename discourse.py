#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 18 17:17:08 2021

@author: emanmarcu
"""

import requests 
import pandas as pd

# Settings
APIKEY="API_KEY"
APIUSERNAME="USERNAME"
QSPARAMS = {"api_key": APIKEY, "api_username": APIUSERNAME}

# =============================================================================
# categories
# =============================================================================
FORUM = "https://community.monday.com/categories.json"
r = requests.get(FORUM, params=QSPARAMS)
categories = r.json()['category_list']['categories']
cats = []
for category in categories:
    cats.append(category)
df_cats = pd.DataFrame(cats)

# =============================================================================
# topics
# =============================================================================
id_name_pairs = tuple(zip(df_cats['id'].values, df_cats['slug'].values))
df_topics = pd.DataFrame()
for pair in id_name_pairs:
    id = str(pair[0])
    slug = str(pair[1])
    
    for j in range(10000000):    
        FORUM = "https://community.monday.com/c/SLUG/ID/l/latest.json?ascending=false&page=PAGE"
        FORUM = FORUM.replace('ID', id )
        FORUM = FORUM.replace('SLUG', slug)
        FORUM = FORUM.replace('PAGE', str(j))
        r = requests.get(FORUM, params=QSPARAMS)
        rj = r.json()
        
        if rj['topic_list']['topics'] == []:
            break
            
        df_topics_cat = pd.DataFrame(rj['topic_list']['topics'])
        df_topics_cat['category_id'] = id
        df_topics_cat['category_slug'] = slug
        df_topics = pd.concat([df_topics, df_topics_cat])
        
# =============================================================================
# posts
# =============================================================================
id_slug_pairs = tuple(zip(df_topics['id'].values, df_topics['slug'].values))
df_posts = pd.DataFrame()
for pair in id_slug_pairs:
    id = str(pair[0])
    FORUM = "https://community.monday.com/t/ID.json"
    FORUM = FORUM.replace('ID', id )
    r = requests.get(FORUM, params=QSPARAMS)
    rj = r.json()
    df_posts_topic = pd.DataFrame(rj['post_stream']['posts'])
    df_posts = pd.concat([df_posts, df_posts_topic])


# =============================================================================
# tags
# =============================================================================
FORUM = "https://community.monday.com/tags.json"
r = requests.get(FORUM, params=QSPARAMS)
rj = r.json()
df_tags = pd.DataFrame(rj['tags'])


# Dump the following tables into BB DB:
#df_cats
#df_topics
#df_posts
#df_tags
