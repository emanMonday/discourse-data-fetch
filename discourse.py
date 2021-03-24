#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import time
# =============================================================================
# api settings
# =============================================================================
APIKEY = 'APIKEY'
USERNAME = 'USERNAME'
HEADERS = {"Api-Key": APIKEY, "Api-Username": USERNAME}
sleep_secs = 0.3

# =============================================================================
# categories
# =============================================================================
FORUM = "https://community.monday.com/categories.json"
r = requests.get(FORUM, headers=HEADERS)
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
        r = requests.get(FORUM, headers=HEADERS)
        rj = r.json()
        
        if rj['topic_list']['topics'] == []:
            break
            
        df_topics_cat = pd.DataFrame(rj['topic_list']['topics'])
        df_topics_cat['category_id'] = id
        df_topics_cat['category_slug'] = slug
        df_topics = pd.concat([df_topics, df_topics_cat])
        
        time.sleep(sleep_secs)
        
# =============================================================================
# posts
# =============================================================================
id_slug_pairs = tuple(zip(df_topics['id'].values, df_topics['slug'].values))
df_posts = pd.DataFrame()
for pair in id_slug_pairs:
    id = str(pair[0])
    FORUM = "https://community.monday.com/t/ID.json"
    FORUM = FORUM.replace('ID', id )
    r = requests.get(FORUM, headers=HEADERS)
    rj = r.json()
    df_posts_topic = pd.DataFrame(rj['post_stream']['posts'])
    df_posts = pd.concat([df_posts, df_posts_topic])

    time.sleep(sleep_secs)

# =============================================================================
# tags
# =============================================================================
FORUM = "https://community.monday.com/tags.json"
r = requests.get(FORUM, headers=HEADERS)
rj = r.json()
df_tags = pd.DataFrame(rj['tags'])


# =============================================================================
# users
# =============================================================================
user_types = ['active', 'new', 'staff', 'suspended', 'blocked', 'suspect']
df_users = pd.DataFrame()
for user_type in user_types:

    for j in range(10000000):   
        FORUM = "https://community.monday.com/admin/users/list/USERTYPE.json?ascending=false&page=PAGE"
        FORUM = FORUM.replace('USERTYPE', user_type)
        FORUM = FORUM.replace('PAGE', str(j))
        r = requests.get(FORUM, headers=HEADERS)
        rj = r.json()

        df_users_type = pd.DataFrame(rj)
        df_users_type['user_type'] = user_type
        df_users = pd.concat([df_users, df_users_type])
        
        time.sleep(sleep_secs)
        if rj == []:
            break        
        
df_users = df_users.drop(['email', 'secondary_emails'], axis=1)
        
user_names =  df_users.username.drop_duplicates().values
emails_dict = {}
for user_name in user_names:
    FORUM = "https://community.monday.com/u/USERNAME/emails.json"
    FORUM = FORUM.replace('USERNAME', user_name)
    r = requests.get(FORUM, headers=HEADERS)
    rj = r.json()
    emails_dict[user_name] = rj

emails = pd.DataFrame(emails_dict).T
emails.index.name = 'username'
emails = emails.reset_index()
    
df_users = pd.merge(df_users, emails, on='username', how='left')


# Dump the following tables into BB DB:
#df_cats
#df_topics
#df_posts
#df_tags
#df_users