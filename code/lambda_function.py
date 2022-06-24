import boto3
import requests
import pandas as pd
import io
import datetime
import json
import os
from db import get_latest_date, instantiate_db, load_to_db
from data_transformation import join_dfs, change_to_datetime, clean_dataframe, rename_cols


s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
sns_client = boto3.client('sns')

def download_file(url, file_name):
    ''' Downloads data from a file and saves it to s3 as an object'''
    body = requests.get(url).content.strip(b'\n')
    bucket_name = os.environ['S3_BUCKET']
    s3_path = "data/" + file_name
    s3_resource.Bucket(bucket_name).put_object(Key=s3_path, Body=body)
    
    
def load_data(key):
    ''' Loads data from s3 into a pandas Dataframe'''
    obj = s3_client.get_object(Bucket=os.environ['S3_BUCKET'], Key=key)
    csv_file = obj['Body'].read().strip(b'\n')
    df = pd.read_csv(io.BytesIO(csv_file), low_memory=False)
    return df
    
def send_db(df):
    ''' Sends the data to the database that only has a date higher than
    current latest in the db'''
    latest = pd.to_datetime(get_latest_date())
    data_to_send = df[df['Date'] > latest]
    data_to_send.apply(lambda x: load_to_db(x), axis=1)
    return len(data_to_send)
    
def post_to_sns(message):
    ''' Sends the output of the job to an SNS topic which fans out
    the result to any number of interested message consumers'''
    response = sns_client.publish(
        TargetArn=os.environ['SNS_TOPIC'],
        Message=json.dumps({'default': json.dumps(message)}),
        MessageStructure='json'
    )
    return response


def lambda_handler(event, context):
    try:
        download_file('https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv', 'data.csv')
        download_file('https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv', 'john_hopkins.csv')
        df = load_data('data/data.csv')
        jh = load_data('data/john_hopkins.csv')
        rename_cols(df, {'date':'Date'})
        clean_dataframe(jh, 'US', ['Deaths', 'Country/Region' ,'Province/State',  'Confirmed'])
        change_to_datetime(df)
        change_to_datetime(jh)
        final = join_dfs(df, jh)
        instantiate_db()
        data_sent = send_db(final)
        res = post_to_sns('Job ran successfully, Updated {} records in the database'.format(data_sent))
    except Exception as e:
        res = post_to_sns('Job failed with error: {}'.format(e))
