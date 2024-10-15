import boto3
from dotenv import load_dotenv
import os

load_dotenv()

# Initialize a session using Amazon DynamoDB with credentials
dynamodb = boto3.client(
    'dynamodb',
    aws_access_key_id=os.getenv('ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('SECRET_ACCESS_KEY'),
    region_name='us-east-2'
)

# Describe the table
table_name = 'notifications'
response = dynamodb.describe_table(TableName=table_name)

# Print the table description
print(response['Table']['KeySchema'])