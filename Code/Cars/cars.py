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

tweepyClient = tweepy.Client(bearer_token='twitter developer account id')
session = boto3.Session(
                     aws_access_key_id='',
                     aws_secret_access_key='')

s3 = session.resource('s3')
twitterIDs = ['66395780', '43430484', '26007726', '23689478', '88803528', '33645850']

def lambda_handler(event, context):
    # TODO implement
    
    dataList = []
    
    
    for Id in twitterIDs:
        test = tweepy.Paginator(tweepyClient.get_users_tweets, id=Id, tweet_fields=['id', 'author_id', 'text', 'attachments', 'context_annotations', 'conversation_id', 'edit_controls', 'entities', 'possibly_sensitive', 'public_metrics', 'referenced_tweets', 'withheld', 'created_at', 'geo'], max_results=100).flatten(limit=6000)
        for t in test:
            dataList.append({"tweetId":t.id, "authorId":t.author_id, "createdDate":t.created_at.isoformat(), "text":t.text, "attachments":t.attachments, "contextAnnotations":t.context_annotations, "conversationId":t.conversation_id, "editControls":t.edit_controls, "entities":t.entities, "possiblySensitive":t.possibly_sensitive, "publicMetrics":t.public_metrics, "referencedTweets":t.referenced_tweets, "withHeld":t.withheld, "geo": t.geo})
    
    x = dumps(dataList, default=str, indent=4)
    
    #object = s3.Object('ccfinalproject6828', 'CARData.json')
    object = s3.Object(
        bucket_name='ccfinalproject6828', 
        key='CARData/CARData.json' #folder
    )
    
    resultPut = object.put(Body=x)
    
    resultGet = object.get()

    datatest = resultGet['Body'].read().decode('utf-8') 
    json_data = json.loads(datatest)
    
    masterDf = None
    warnings.filterwarnings('ignore')
    
    df = pd.json_normalize(json_data)
    
    df.drop(['contextAnnotations', 'referencedTweets', 'entities.mentions', 'entities.hashtags', 'attachments', 'withHeld', 'entities.annotations', 'entities.urls', 'attachments.media_keys', 'entities'], axis=1, inplace=True)
    
    if 'geo' in df.columns:
        df.drop(['geo'], axis=1, inplace=True)
    if 'attachments.poll_ids' in df.columns:
        df.drop(['attachments.poll_ids'], axis=1, inplace=True)
    if 'geo.place_id' in df.columns:
        df.drop(['geo.place_id'], axis=1, inplace=True)
    
    df.rename(columns = {'editControls.edits_remaining':'editsRemaining', 'editControls.is_edit_eligible':'editEligible', 'editControls.editable_until':'editableUntil', 'publicMetrics.retweet_count':'retweetCount', 'publicMetrics.reply_count':'replyCount', 'publicMetrics.like_count':'likeCount', 'publicMetrics.quote_count':'quoteCount'}, inplace = True)
    df['text'] = df['text'].astype('string')
    df['createdDate'] = pd.to_datetime(df['createdDate'])
    df['editableUntil'] = pd.to_datetime(df['editableUntil'])
    
    df['createdDate'] = pd.to_datetime(df['createdDate']).dt.date
    date = pd.Timestamp('2022-09-13 00:00:00.000000').date()
    filtered_df = df.loc[(df['createdDate'] >= date)]
    
    test1 = (filtered_df.groupby(['authorId']).count().reset_index())
    test1 = test1.replace([66395780, 43430484, 26007726, 23689478, 88803528, 33645850],['Chevrolet', "Honda", 'Hyundai', 'Kia', 'Mazda', 'Nissan'])
    pdf = test1
    pdf.rename(columns = {'authorId':'Company Name', 'tweetId':'No. of Tweets'}, inplace = True)
    fig = px.bar(pdf, x="Company Name", y="No. of Tweets", title='Number Of Tweets In Last Six Months')
    
    os.chdir('/tmp')    #This is important
    
    #Make a directory
    if not os.path.exists(os.path.join('mydir')):
        os.makedirs('mydir')
    
    filename = 'CarCompanyActivity.html'
    save_path = os.path.join(os.getcwd(), 'mydir', filename)
    fig.write_html(save_path)
    
    lst = os.listdir("/tmp/mydir")
    
    client = boto3.client('s3',
                        aws_access_key_id = '',
                        aws_secret_access_key = '')

    os.chdir('/tmp/mydir')

    client.upload_file(
        Filename="CarCompanyActivity.html",
        Bucket="ccfinalproject6828",
        Key="CAROutput/CarCompanyActivity.html", #folder
         )
         
    clientS3 = boto3.client('s3',
                        aws_access_key_id = '',
                        aws_secret_access_key = '')
                        

    clientS3.copy_object(Key='CAROutput/CarCompanyActivity.html', Bucket='ccfinalproject6828', #folder
                          CopySource={"Bucket": 'ccfinalproject6828', "Key": 'CAROutput/CarCompanyActivity.html'}, #folder
                          ContentType='text/html',
                          ContentDisposition='inline', 
                          ACL='public-read', # makes the object publicly readable
                          MetadataDirective="REPLACE")
                          
    client.put_object_acl(
        ACL="public-read", Bucket="ccfinalproject6828", Key="CAROutput/CarCompanyActivity.html" #folder
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
