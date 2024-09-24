import boto3
import json
import os

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    body = json.loads(event['body'])
    upload_id = body['uploadId']
    parts = body['parts']  # Array of dicts {PartNumber, ETag}

    # Hard-coded file name from environment variable
    file_name = os.environ.get('FILE_NAME', 'survey.csv')
    bucket_name = os.environ['BUCKET_NAME']
    key = f'raw/{file_name}'

    # Complete the multipart upload
    multipart_upload = {
        'Parts': [
            {'ETag': part['ETag'], 'PartNumber': part['PartNumber']}
            for part in parts
        ]
    }

    response = s3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload=multipart_upload
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Multipart upload completed successfully"}),
        "headers": {}
    }
