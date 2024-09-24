import json
import boto3
import os

step_function = json.loads(os.environ['STEP_FUNCTION_ARN'])   

def lambda_handler(event, context):
    stepfunctions = boto3.client('stepfunctions')
    
    # Get job_id from query parameters
    job_id = event['queryStringParameters'].get('jobId')
    # job_id = "2d5b475a-20c0-4240-91d3-ec09291b5355"
    
    if not job_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'jobId is required'}),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
    
    # Construct execution name from job_id
    execution_name = f"processing-job-{job_id}"
    
    # Describe the execution
    try:
        response = stepfunctions.describe_execution(
            executionArn=f"{step_function}:{execution_name}"
        )
        
        status = response['status']
        output = response.get('output', None)
        
        result = {
            'job_id': job_id,
            'status': status
        }
        
        if status == 'SUCCEEDED' and output:
            # Parse the Step Function's output as JSON
            output_json = json.loads(output)
            
            # Check if lambda2_result is present and contains 'body'
            if "lambda2_result" in output_json and "body" in output_json["lambda2_result"]:
                lambda2_body = output_json["lambda2_result"]["body"]
                result = {
                    'status':status,
                    'output':lambda2_body
                }
                
                return {
                    'statusCode': 200,
                    'body': json.dumps(result),
                    'headers': {
                        'Content-Type': 'application/json'
                    }
                }
            else:
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': 'lambda2_result or body not found in output'}),
                    'headers': {
                        'Content-Type': 'application/json'
                    }
                }
        elif status == 'FAILED':
            result['error'] = response.get('error', 'Unknown error')
            result['cause'] = response.get('cause', 'No cause provided')
        
        return {
            'statusCode': 200,
            'body': json.dumps(result),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
        
    except stepfunctions.exceptions.ExecutionDoesNotExist:
        return {
            'statusCode': 404,
            'body': json.dumps({'error': 'Job not found'}),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
