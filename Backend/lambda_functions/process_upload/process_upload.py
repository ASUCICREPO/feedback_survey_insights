import boto3
import json
import os

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        upload_id = body['uploadId']
        parts = body['parts']  # List of part numbers

        # Hard-coded file name from environment variable
        file_name = os.environ.get('FILE_NAME', 'survey.csv')
        bucket_name = os.environ['BUCKET_NAME']
        key = f'raw/{file_name}'

        # Generate pre-signed URLs for each part
        presigned_urls = []
        for part_number in parts:
            presigned_url = s3_client.generate_presigned_url(
                'upload_part',
                Params={
                    'Bucket': bucket_name,
                    'Key': key,
                    'UploadId': upload_id,
                    'PartNumber': part_number,
                },
                ExpiresIn=3600  # URL valid for 1 hour
            )
            presigned_urls.append({'partNumber': part_number, 'url': presigned_url})

        return {
            "statusCode": 200,
            "body": json.dumps({"presignedUrls": presigned_urls}),
            "headers": {}
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
            "headers": {}
        }
