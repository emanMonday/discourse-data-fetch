#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
import json
import pandas as pd 
import numpy as np
from functions import get_query, run_sequential_queries, get_insert_query

table_names = ['tags', 
                 'posts', 
                 'topic_tags', 
                 'users', 
                 'user_actions', 
                 'user_badges', 
                 'topics', 
                 'badges', 
                 'categories', 
                 'topic_views', 
                ]

insert_queries = {}
for table_name in table_names:
    query = get_query(table_name)
    df = run_sequential_queries(query, np, pd, json, requests)
    insert_queries[table_name] = get_insert_query(table_name, df)
    
