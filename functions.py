#!/usr/bin/env python3
# -*- coding: utf-8 -*-

def get_query(typ):
    
    queries = {}
    
    queries['topics'] = '''
            SELECT t.*
            FROM topics t
            join posts p
            on t.id = p.topic_id 
            and p.post_number = 1
            and p.user_id > 0
            order by t.created_at
            '''
    
    queries['posts'] = '''
            with _topics as (
                SELECT distinct t.id as topic_id
                FROM topics t
                join posts p
                on t.id = p.topic_id 
                and p.post_number = 1
                and p.user_id > 0
                )
            
            select p.*
            from posts p
            join _topics t
                on t.topic_id = p.topic_id 
                and p.user_id > 0
            order by p.id
            '''
    
    queries['topic_tags'] = '''
            with _topics as (
                SELECT distinct t.id as topic_id
                FROM topics t
                join posts p
                on t.id = p.topic_id 
                and p.post_number = 1
                and p.user_id > 0
                )
            select tt.*
            from topic_tags tt
            join _topics t
                on t.topic_id = tt.topic_id 
            order by tt.id
            '''
    
    queries['users'] = '''
            select ue.email 
                    , u.*
            from users u
            join user_emails ue
                on u.id = ue.user_id
                and u.id > 0
            order by u.id
            '''
    
    queries['user_badges'] = '''
            select *
            from user_badges
            where user_id > 0
            order by id
            '''
        
    queries['user_actions'] = '''
            select *
            from user_actions
            where user_id > 0
            order by id
            '''
            
    queries['badges'] = '''
            select *
            from badges
            order by id
            '''
            
    queries['tags'] = '''
            select *
            from tags
            order by id
            '''
    
    queries['categories'] = '''
            select *
            from categories
            order by id
            '''
        
    return queries[typ]

def run_sequential_queries(query, np, pd, json, requests):
 
    API_KEY = 'API_KEY'
    API_USERNAME = 'API_USERNAME'
    headers = {'Content-Type': 'multipart/form-data', 'Api-Key': API_KEY, 'Api-Username': API_USERNAME}

    query += ' limit 1000 offset OFFSET'
    rang = np.arange(0,10000000000, 1000)
    df = pd.DataFrame()
    
    for offset in rang:
        # UPDATE
        ENDPOINT = "https://community.monday.com/admin/plugins/explorer/queries/28"
        query_to_request = query.replace('OFFSET', str(int(offset)))
        request = requests.put(ENDPOINT, 
                               headers=headers, 
                               data="query[sql]="+query_to_request) # results in  <Response [400]>
        # RUN
        ENDPOINT = "https://community.monday.com/admin/plugins/explorer/queries/28/run"
        request = requests.post(ENDPOINT, headers=headers)
        data = request.content
        x = json.loads(data.decode())
        df_offset = pd.DataFrame(x['rows'], columns=x['columns'])
        if df_offset.shape[0] == 0:
            break
        df = pd.concat([df, df_offset])
    return df
