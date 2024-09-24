import boto3
import os
import json

def lambda_handler(event, context):
    sagemaker_client = boto3.client('sagemaker')
    job_id = event.get('job_id')
    objectName = event.get('object_name')

    processing_job_name = f"processing-job-{job_id}"
    bucket_name = os.environ['BUCKET_NAME']
    docker_image_uri = os.environ['DOCKER_IMAGE_URI']
    role_arn = os.environ['SAGEMAKER_ROLE_ARN']
    object_name = objectName

    response = sagemaker_client.create_processing_job(
        ProcessingJobName=processing_job_name,
        RoleArn=role_arn,
        AppSpecification={
            'ImageUri': docker_image_uri,
            'ContainerEntrypoint': ['python3', '/opt/ml/processing/input/code/processing_script.py'],
            'ContainerArguments': [
                '--input-data', '/opt/ml/processing/input/data',
                '--output-data', '/opt/ml/processing/output',
                '--object-name', object_name
            ]
        },
        ProcessingResources={
            'ClusterConfig': {
                'InstanceCount': 1,
                'InstanceType': 'ml.c5.xlarge',
                'VolumeSizeInGB': 10
            }
        },
        ProcessingInputs=[
            {
                'InputName': 'input-data',
                'AppManaged': False,
                'S3Input': {
                    'S3Uri': f's3://{bucket_name}/filter/',
                    'LocalPath': '/opt/ml/processing/input/data',
                    'S3DataType': 'S3Prefix',
                    'S3InputMode': 'File'
                }
            },
            {
                'InputName': 'code',
                'AppManaged': False,
                'S3Input': {
                    'S3Uri': f's3://{bucket_name}/scripts/processing_script.py',
                    'LocalPath': '/opt/ml/processing/input/code',
                    'S3DataType': 'S3Prefix',
                    'S3InputMode': 'File'
                }
            }
        ],
        ProcessingOutputConfig={
            'Outputs': [
                {
                    'OutputName': 'output-data',
                    'S3Output': {
                        'S3Uri': f's3://{bucket_name}/processed/',
                        'LocalPath': '/opt/ml/processing/output',
                        'S3UploadMode': 'Continuous'
                    }
                }
            ]
        },
        StoppingCondition={
            'MaxRuntimeInSeconds': 3600
        }
    )

    return {
        'statusCode': 200,
        'processing_job_name': processing_job_name,
        'sagemaker_response': response
    }
