# Lambda Function for Loading big data to S3

This AWS Lambda function is designed to handle S3 event notifications, execute Redshift queries based on the S3 object key, and manage the results by storing them in DynamoDB and sending SNS notifications.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Environment Variables](#environment-variables)
- [Dependencies](#dependencies)
- [Function Details](#function-details)
- [Error Handling](#error-handling)
- [Deployment](#deployment)
- [Usage](#usage)

## Overview

This Lambda function performs the following tasks:

1. Retrieves and increments a counter from environment variables.
2. Processes the S3 event notification.
3. Constructs and executes Redshift SQL queries based on the S3 object key.
4. Stores the results in DynamoDB if the query fails.
5. Sends an SNS notification if the query fails.


## Environment Variables

- `NUMBER`: A counter that is incremented with each invocation.
- `AWS_REGION`: The AWS region where resources are deployed.

## Dependencies

The Lambda function requires the following AWS SDKs:

- `boto3`
- `botocore`

These are included in the AWS Lambda runtime environment. Ensure your Lambda function has the appropriate IAM permissions to access S3, Redshift, DynamoDB, and SNS.

## Function Details

### `lambda_handler(event, context)`

The main entry point for the Lambda function. It processes the S3 event and triggers the Redshift execution.

#### Parameters

- `event`: The event data from S3.
- `context`: The runtime information of the Lambda function.

#### Returns

A JSON response with a status code and message.

### `store_to_dynamodb(s3_object_key, status, sql_statements)`

Stores the failed SQL query information in DynamoDB.

#### Parameters

- `s3_object_key`: The key of the S3 object.
- `status`: The status of the Redshift query execution.
- `sql_statements`: The SQL statements executed.

### `send_sns_notification(s3_object_key, status, sql_statements)`

Sends an SNS notification if the Redshift query fails.

#### Parameters

- `s3_object_key`: The key of the S3 object.
- `status`: The status of the Redshift query execution.
- `sql_statements`: The SQL statements executed.

### `exec_redshift(s3_object_key)`

Constructs and executes Redshift SQL queries based on the S3 object key.

#### Parameters

- `s3_object_key`: The key of the S3 object.

#### Returns

The status of the SQL execution.

## Error Handling

If the Redshift query fails, the function stores the failure details in DynamoDB and sends an SNS notification.

## Deployment

1. Create an IAM role with the necessary permissions for S3, Redshift, DynamoDB, and SNS.
2. Create a Lambda function with this code and attach the IAM role.
3. Set up the environment variables.
4. Configure S3 to trigger the Lambda function on object creation events.

## Usage

To use this Lambda function, configure your S3 bucket to send event notifications to this Lambda function. The function will handle the events, execute the necessary Redshift commands, and manage any errors accordingly.
