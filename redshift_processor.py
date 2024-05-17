import json
import os
from datetime import datetime
import boto3
import botocore 
import botocore.session as bc
from botocore.client import Config
import time                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
def lambda_handler(event, context):
    # Retrieve the current value of the number from environment variables
    number = int(os.environ.get('NUMBER', '0'))

    # Increment the number                                                                             
    number += 1

    # Store the updated number back to environment variables
    os.environ['NUMBER'] = str(number)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # TODO implement
    print(f"Event_{number}_time: {current_time} : ", event)
    s3_object_key = event['Records'][0]['s3']['object']['key']
    exec_redshift(s3_object_key)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def store_to_dynamodb(s3_object_key, status, sql_statements):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('customer-io-failed-queries')  

    response = table.put_item(
        Item={
            'object_key': s3_object_key,
            'status': status,
            'query' : sql_statements
        }
    )

    print("DynamoDB PutItem response:", response)


def send_sns_notification(s3_object_key, status, sql_statements):
    sns_client = boto3.client('sns')
    topic_arn = 'arn:aws:sns:us-east-1:459242206444:compliance-notifications' 
    message = f"Redshift query failed for object key: {s3_object_key}\nStatus: {status}\nQuery: {sql_statements}"
    
    sns_client.publish(
        TopicArn=topic_arn,
        Message=message,
        Subject='Redshift Query Failure Notification'
    )

def exec_redshift(s3_object_key):
    sql_statements = ''
        # Define the SQL statements based on the S3 object uploaded
    if 'attributes' in s3_object_key:
        sql_statements = f"""
            COPY attributes(workspace_id, internal_customer_id, attribute_name, attribute_value, timestamp)
            FROM 's3://customer-io-data/{s3_object_key}'
            FORMAT AS PARQUET
            IAM_ROLE 'arn:aws:iam::459242206444:role/service-role/AmazonRedshift-CommandsAccessRole-20220408T123304';
        """
    elif 'deliveries' in s3_object_key:
        sql_statements = f"""
            COPY deliveries(workspace_id, delivery_id, internal_customer_id, subject_id, event_id, delivery_type, campaign_id, action_id, newsletter_id, content_id, trigger_id, created_at, transactional_message_id, seq_num)
            FROM 's3://customer-io-data/{s3_object_key}'
            FORMAT AS PARQUET
            IAM_ROLE 'arn:aws:iam::459242206444:role/service-role/AmazonRedshift-CommandsAccessRole-20220408T123304';
        """
    elif 'people' in s3_object_key:
        sql_statements = f"""
            COPY People ( workspace_id, customer_id, internal_customer_id,deleted, suppressed, created_at, updated_at, email_addr)
            FROM 's3://customer-io-data/{s3_object_key}'
            FORMAT AS PARQUET
            IAM_ROLE 'arn:aws:iam::459242206444:role/service-role/AmazonRedshift-CommandsAccessRole-20220408T123304';
        """ 
  
    elif 'delivery_contents' in s3_object_key:
        sql_statements = f"""
            COPY Delivery_Content (workspace_id, delivery_id, campaign_id, action_id, newsletter_id, content_id, "from", reply_to, bcc, recipient, subject, body, body_amp, body_plain, preheader, url, method, headers, type)
            FROM 's3://customer-io-data/{s3_object_key}'
            FORMAT AS PARQUET
            IAM_ROLE 'arn:aws:iam::459242206444:role/service-role/AmazonRedshift-CommandsAccessRole-20220408T123304';
        """
    elif 'outputs' in s3_object_key:
        sql_statements = f"""
            COPY Outputs (workspace_id, output_id, subject_name, output_type, action_id, explanation, delivery_id, draft, link_tracked, split_test_index, delay_ends_at, branch_index, manual_segment_id, add_to_manual_segment, created_at, seq_num )
            FROM 's3://customer-io-data/{s3_object_key}'
            FORMAT AS PARQUET
            IAM_ROLE 'arn:aws:iam::459242206444:role/service-role/AmazonRedshift-CommandsAccessRole-20220408T123304';
        """
    elif 'metrics' in s3_object_key:
        sql_statements = f"""
            COPY Metrics( event_id, workspace_id, delivery_id, metric, created_at, reason, link_id, link_url, seq_num)
            FROM 's3://customer-io-data/{s3_object_key}'
            FORMAT AS PARQUET
            IAM_ROLE 'arn:aws:iam::459242206444:role/service-role/AmazonRedshift-CommandsAccessRole-20220408T123304';
        """
    elif 'subjects' in s3_object_key:
        sql_statements = f"""
            COPY Subjects ( workspace_id, subject_name, internal_customer_id, campaign_type, campaign_id, event_id, trigger_id, started_campaign_at, created_at, seq_num )
            FROM 's3://customer-io-data/{s3_object_key}'
            FORMAT AS PARQUET
            IAM_ROLE 'arn:aws:iam::459242206444:role/service-role/AmazonRedshift-CommandsAccessRole-20220408T123304';
        """
    
    
    host='customerio-warehouse-redshift-cluster-1.cccxeoik9uo0.us-east-1.redshift.amazonaws.com',
    port=5439,
    user='awsuser',
    password='123456789Aa',
    database='dev'
    


    pwd = password
    cluster_name = "customerio-warehouse-redshift-cluster-1"
    db_name = database
    db_user = user

    # sql_statements = "select * from people;"
    # sql_statements = """
    # COPY attributes(workspace_id, internal_customer_id, attribute_name, attribute_value, timestamp)
    # FROM 's3://customer-io-data/attributes_v4_111528_241600000000311123.parquet'
    # FORMAT AS PARQUET
    # IAM_ROLE 'arn:aws:iam::459242206444:role/service-role/AmazonRedshift-CommandsAccessRole-20220408T123304';
    # """ 
    print("SQL STATEMENT: ", sql_statements)
    redshift_data_client = boto3.client("redshift-data")
    
    print("Client: ", redshift_data_client)

    result = redshift_data_client.execute_statement(
        ClusterIdentifier = "customerio-warehouse-redshift-cluster-1",
        Database='dev',
        DbUser='awsuser',
        Sql=sql_statements

    )
    
    print("result: ", result)
    id = result['Id']
    statement = ''
    status = ''

    # We have to wait in loop for the sql commands to finish executing
    while status != 'FINISHED' and status != 'FAILED' and status != 'ABORTED':
        statement = redshift_data_client.describe_statement(Id=id)
        status = statement['Status']
        time.sleep(2)

    status = statement['Status']
    # store_to_dynamodb(s3_object_key, status, sql_statements)
    # send_sns_notification(s3_object_key, status, sql_statements)

    if status == "FAILED":
        store_to_dynamodb(s3_object_key, status,sql_statements )
        send_sns_notification(s3_object_key, status, sql_statements)
        raise Exception(statement['Error'])
        
    print("Query status: ", status)

    return status
