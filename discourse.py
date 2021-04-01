#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
import json
import pandas as pd 
import numpy as np
from functions import get_query, run_sequential_queries

types = ['topics', 
         'posts', 
         'topic_tags', 
         'users', 
         'user_actions', 
         'user_badges', 
         'tags', 
         'badges', 
         'categories', 
         'topic_views', 
        ]


dfs = {}
for typ in types:
    query = get_query(typ)
    dfs[typ] = run_sequential_queries(query, np, pd, json, requests)

# dfs is a dict of data frames. we want to send all of these to BB DB