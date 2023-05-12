import json
import json
import boto3
import os
import pandas as pd
import os
from json import load
import plotly.express as px
import warnings
import time
import tweepy
from json import dump, dumps
import plotly.graph_objects as go

tweepyClient = tweepy.Client(bearer_token='')
session = boto3.Session(
                     aws_access_key_id='',
                     aws_secret_access_key='')

s3 = session.resource('s3')

def lambda_handler(event, context):
    # TODO implement
    query = 'WWEBacklash OR #WWEBacklash OR #wwebacklash OR #WWEBACKLASH -is:retweet lang:en'
    dataList = []
    
    startTimeList = ['2023-05-06T05:00:00Z', '2023-05-06T08:30:00Z', '2023-05-06T12:30:00Z', '2023-05-06T16:30:00Z']
    endTimeList = ['2023-05-06T07:00:00Z', '2023-05-06T10:30:00Z', '2023-05-06T14:30:00Z', '2023-05-06T18:30:00Z']

    for timeIndex in range(0,len(startTimeList)):
        test = tweepy.Paginator(tweepyClient.search_recent_tweets, query, end_time=endTimeList[timeIndex], start_time=startTimeList[timeIndex], tweet_fields=['id', 'author_id', 'text', 'attachments', 'context_annotations', 'conversation_id', 'edit_controls', 'entities', 'possibly_sensitive', 'public_metrics', 'referenced_tweets', 'withheld', 'created_at', 'geo'], max_results=100).flatten(limit=6000)
        for t in test:
            dataList.append({"tweetId":t.id, "authorId":t.author_id, "createdDate":t.created_at.isoformat(), "text":t.text, "attachments":t.attachments, "contextAnnotations":t.context_annotations, "conversationId":t.conversation_id, "editControls":t.edit_controls, "entities":t.entities, "possiblySensitive":t.possibly_sensitive, "publicMetrics":t.public_metrics, "referencedTweets":t.referenced_tweets, "withHeld":t.withheld, "geo": t.geo})

    x = dumps(dataList, default=str, indent=4)
    
    #object = s3.Object('ccfinalproject01', 'WWEBacklashData.json')
    object = s3.Object(
        bucket_name='ccfinalproject01', 
        key='WWEBacklashData/WWEBacklashData.json' #folder
    )
    
    resultPut = object.put(Body=x)
    #object = s3.Object('ccfinalproject01', 'WWEBacklashData.json')

    resultGet = object.get()

    datatest = resultGet['Body'].read().decode('utf-8') 
    json_data = json.loads(datatest)
    
    masterDf = None
    warnings.filterwarnings('ignore')
    
    df = pd.json_normalize(json_data)
    df.drop(['contextAnnotations', 'referencedTweets', 'entities.mentions', 'withHeld', 'entities.hashtags', 'entities.cashtags', 'attachments', 'entities.annotations', 'entities.urls', 'attachments.media_keys'], axis=1, inplace=True)
        
    if 'geo' in df.columns:
        df.drop(['geo'], axis=1, inplace=True)
    if 'attachments.poll_ids' in df.columns:
        df.drop(['attachments.poll_ids'], axis=1, inplace=True)
    if 'geo.place_id' in df.columns:
        df.drop(['geo.place_id'], axis=1, inplace=True)
    if 'withHeld.copyright' in df.columns:
        df.drop(['withHeld.copyright'], axis=1, inplace=True)
    if 'withHeld.country_codes' in df.columns:
        df.drop(['withHeld.country_codes'], axis=1, inplace=True)
    if 'entities' in df.columns:
        df.drop(['entities'], axis=1, inplace=True)
        
    df.rename(columns = {'editControls.edits_remaining':'editsRemaining', 'editControls.is_edit_eligible':'editEligible', 'editControls.editable_until':'editableUntil', 'publicMetrics.retweet_count':'retweetCount', 'publicMetrics.reply_count':'replyCount', 'publicMetrics.like_count':'likeCount', 'publicMetrics.quote_count':'quoteCount'}, inplace = True)
    df['text'] = df['text'].astype('string')
    df['createdDate'] = pd.to_datetime(df['createdDate'])
    df['editableUntil'] = pd.to_datetime(df['editableUntil'])
    #print(df.infer_objects().dtypes)
    
    test1 = (df.groupby(['possiblySensitive']).count().reset_index())
    fig = px.bar(test1, x="possiblySensitive", y="tweetId", labels={
                     "possiblySensitive": "Possibly Sensitive",
                     "tweetId": "Number of Tweets"
                 },title='Possibly Sensitive Tweet Count: WWEBacklash')
                 
    #Most Liked Tweets
    df1 = df[df['likeCount']==df['likeCount'].max()]

    df5 = df1[['tweetId', 'authorId', 'createdDate', 'text', 'likeCount']]

    fig1 = go.Figure(data=[go.Table(
    header=dict(values=list(df5.columns),
                fill_color='paleturquoise',
                align='left'),
    cells=dict(values=[df5.tweetId, df5.authorId, df5.createdDate, df5.text, df5.likeCount],
               fill_color='lavender',
               align='left'))
    ])

    os.chdir('/tmp')    #This is important
    
    #Make a directory
    if not os.path.exists(os.path.join('mydir')):
        os.makedirs('mydir')
    
    filename = 'PossiblySensitive.html'
    save_path = os.path.join(os.getcwd(), 'mydir', filename)
    fig.write_html(save_path)
    
    os.chdir('/tmp')    #This is important
    
    #Make a directory
    if not os.path.exists(os.path.join('mydir')):
        os.makedirs('mydir')
    
    filename = 'WWEBacklashMostLiked.html'
    save_path = os.path.join(os.getcwd(), 'mydir', filename)
    fig1.write_html(save_path)
    
    lst = os.listdir("/tmp/mydir")
    
    client = boto3.client('s3',
                        aws_access_key_id = '',
                        aws_secret_access_key = '')

    os.chdir('/tmp/mydir')

    client.upload_file(
        Filename="PossiblySensitive.html",
        Bucket="ccfinalproject01",
        Key="WWEBacklashOutput/PossiblySensitiveUpload.html", #folder
         )
    
    client.upload_file(
        Filename="WWEBacklashMostLiked.html",
        Bucket="ccfinalproject01",
        Key="WWEBacklashOutput/WWEBacklashMostLikedUpload.html", #folder
         )
    
    
    clientS3 = boto3.client('s3',
                        aws_access_key_id = '',
                        aws_secret_access_key = '')
                        
    #s3_object = clientS3.Object('ccfinalproject01', 'PossiblySensitiveUpload.html')
    #s3_object.metadata.update({'Content-Type':'text/html'})
    #s3_object.copy_from(CopySource={'Bucket':'ccfinalproject01', 'Key':'CCFinal.html'}, Metadata=s3_object.metadata, MetadataDirective='REPLACE')

    clientS3.copy_object(Key='WWEBacklashOutput/PossiblySensitiveUpload.html', Bucket='ccfinalproject01', #folder
                          CopySource={"Bucket": 'ccfinalproject01', "Key": 'WWEBacklashOutput/PossiblySensitiveUpload.html'}, #folder
                          ContentType='text/html',
                          ContentDisposition='inline', 
                          ACL='public-read', # makes the object publicly readable
                          MetadataDirective="REPLACE")
                          
    client.put_object_acl(
        ACL="public-read", Bucket="ccfinalproject01", Key="WWEBacklashOutput/PossiblySensitiveUpload.html" #folder
    )
    
    
    clientS3.copy_object(Key='WWEBacklashOutput/WWEBacklashMostLikedUpload.html', Bucket='ccfinalproject01', #folder
                          CopySource={"Bucket": 'ccfinalproject01', "Key": 'WWEBacklashOutput/WWEBacklashMostLikedUpload.html'}, #folder
                          ContentType='text/html',
                          ContentDisposition='inline', 
                          ACL='public-read', # makes the object publicly readable
                          MetadataDirective="REPLACE")
                          
    client.put_object_acl(
        ACL="public-read", Bucket="ccfinalproject01", Key="WWEBacklashOutput/WWEBacklashMostLikedUpload.html" #folder
    )
    
    time.sleep(600)
    
    return {
        'statusCode': 200,
        'body': lst
    }
