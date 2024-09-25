import boto3
import json
import os

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Hard-coded file name and type from environment variables
    file_name = os.environ.get('FILE_NAME', 'survey.csv')
    file_type = os.environ.get('FILE_TYPE', 'text/csv')

    # Retrieve bucket name from environment variable
    bucket_name = os.environ['BUCKET_NAME']
    key = f'raw/{file_name}'

    # Initiate multipart upload
    response = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        ContentType=file_type
    )
    upload_id = response['UploadId']

    return {
        "statusCode": 200,
        "body": json.dumps({
            "uploadId": upload_id,
            "fileName": file_name
        }),
        "headers": {
            "Access-Control-Allow-Origin": "*", 
            "Access-Control-Allow-Methods": "POST",
            "Access-Control-Allow-Headers": "Content-Type"
        }
    }
