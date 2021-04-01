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
            
    queries['topic_views'] = '''
            select topic_id, user_id, viewed_at
            from topic_views
            order by 1,2,3 
            '''
        
    return queries[typ]

def run_sequential_queries(query, np, pd, json, requests):
    import time
    sec_sleep = 0.25
    API_KEY = 'API_KEY'
    API_USERNAME = 'API_USERNAME'
    headers = {'Content-Type': 'multipart/form-data', 'Api-Key': API_KEY, 'Api-Username': API_USERNAME}

    query += ' limit 1000 offset OFFSET'
    rang = np.arange(0,10000000000, 1000)
    df = pd.DataFrame()
    
    for offset in rang:
        ENDPOINT = "https://community.monday.com/admin/plugins/explorer/queries/28"
        query_to_request = query.replace('OFFSET', str(int(offset)))
        request = requests.put(ENDPOINT, 
                               headers=headers, 
                               data="query[sql]="+query_to_request)
        ENDPOINT = "https://community.monday.com/admin/plugins/explorer/queries/28/run"
        request = requests.post(ENDPOINT, headers=headers)
        data = request.content
        x = json.loads(data.decode())
        try:
            df_offset = pd.DataFrame(x['rows'], columns=x['columns'])
        except KeyError:
            print(data)
            print(x)
            raise KeyError
        if df_offset.shape[0] == 0:
            break
        df = pd.concat([df, df_offset])
        time.sleep(sec_sleep)
        
        
    return df



def get_queue(typ):
 
    queries = {}
    
    # Q1: Ghost topics
    queries['q1'] = '''
                    select t.id as topic_id
                            , t.created_at
                            , t.title
                            , c.name as category
                            , COALESCE(d.votes_count, 0) as votes_count
                            , t.like_count
                            , t.views
                            , t.last_posted_at
                            , p.raw
                    from topics t
                    join posts p
                        on t.id = p.topic_id
                        and p.post_number = 1
                    join user_emails ue
                        on t.user_id = ue.user_id
                        and SUBSTRING(ue.email, POSITION('@' IN ue.email) %2B 1, length(ue.email)) != 'monday.com'
                    left join categories c 
                        on t.category_id = c.id
                    left join discourse_voting_topic_vote_count d
                        on d.topic_id = t.id
                    where t.posts_count = 1
                    and t.reply_count = 0
                    and t.created_at < now() - interval '7' day
                    and t.closed = 'false'
                    and t.user_id > 0
                    and t.deleted_at is null
                    and t.visible = 'true'
                    and t.archived = 'false'
                    and t.id not in ( select distinct target_topic_id from user_actions where action_type=15)
                    and t.subtype is null
                    order by t.id desc                    

                    '''
                    
    # Q2: Multiple posts and last post is by user   
    queries['q2'] = '''
                
                    with 
                    posts_by_user as (
                        select t.posts_count
                                , t.id as topic_id
                                , t.created_at
                                , t.title
                                , c.name as category
                                , t.like_count
                                , t.views
                                , t.last_posted_at     
                                , COALESCE(d.votes_count, 0) as votes_count
                                , p2.raw
                        from posts p
                        join user_emails ue
                            on p.user_id = ue.user_id 
                            and SUBSTRING(ue.email, POSITION('@' IN ue.email) %2B 1, length(ue.email)) != 'monday.com'
                        join topics t  
                            on p.topic_id = t.id
                            and ( t.posts_count > 1 or t.reply_count > 0 )
                            and t.created_at < now() - interval '7' day
                            and t.closed = 'false'
                            and t.deleted_at is null
                            and t.visible = 'true'
                            and t.archived = 'false'
                            and t.id not in ( select distinct target_topic_id from user_actions where action_type=15)
                            and t.subtype is null
                        join posts p2 
                            on p2.topic_id = t.id 
                            and p2.post_number = t.posts_count
                        left join categories c 
                            on t.category_id = c.id
                        left join discourse_voting_topic_vote_count d
                            on d.topic_id = t.id
                        where p.post_number = 1
                        and p.user_id > 0
                        )
                    
                    select p.* 
                    from posts_by_user p
                    join topics t
                        on p.topic_id = t.id
                    join user_emails ue 
                        on t.last_post_user_id = ue.user_id 
                            and SUBSTRING(ue.email, POSITION('@' IN ue.email) %2B 1, length(ue.email)) != 'monday.com'
                    order by p.created_at desc
                        
                    '''   
                    
                    
    # Q3: Missing our followup on topics generated by us              
    queries['q3'] = '''
                
                    with 
                    posts_by_us as (
                        select t.posts_count
                                , t.id as topic_id
                                , t.created_at
                                , t.title
                                , c.name as category
                                , COALESCE(d.votes_count, 0) as votes_count
                                , t.like_count
                                , t.views
                                , t.last_posted_at
                                , p2.raw
                        from posts p
                        join user_emails ue
                            on p.user_id = ue.user_id 
                            and SUBSTRING(ue.email, POSITION('@' IN ue.email) %2B 1, length(ue.email)) = 'monday.com'
                        join topics t  
                            on p.topic_id = t.id
                            and ( t.posts_count > 1 or t.reply_count > 0 )
                            and t.created_at < now() - interval '7' day
                            and t.closed = 'false'
                            and t.deleted_at is null
                            and t.visible = 'true'
                            and t.archived = 'false'
                            and t.id not in ( select distinct target_topic_id from user_actions where action_type=15)
                            and t.subtype is null
                        join posts p2 
                            on p2.topic_id = t.id 
                            and p2.post_number = t.posts_count
                        left join categories c 
                            on t.category_id = c.id
                        left join discourse_voting_topic_vote_count d
                            on d.topic_id = t.id
                        where p.post_number = 1
                        and p.user_id > 0
                        )
                    
                        select p.* 
                        from posts_by_us p
                        join topics t
                            on p.topic_id = t.id
                        join user_emails ue 
                            on t.last_post_user_id = ue.user_id 
                                and SUBSTRING(ue.email, POSITION('@' IN ue.email) %2B 1, length(ue.email)) != 'monday.com'
                        order by p.created_at desc
                        
                    '''
                   
    return queries[typ]






def get_queue_tags(topics, np, pd, json, requests):
    
    topic_ids_str = str(topics)[1:-1]        
    
    query = '''
    
        select tt.topic_id
                ,t.name
        from topic_tags tt
        join tags t 
            on tt.tag_id = t.id
        where tt.topic_id in (TOPIC_IDS)
        order by 1,2 
        
        '''
                
            
    query = query.replace('TOPIC_IDS', topic_ids_str)
    sd = run_sequential_queries(query, np, pd, json, requests)
    return sd
    
    
    
    
    
    
    
    
    
    
    
    
    
    