import json
import boto3
import uuid
from botocore.exceptions import ClientError
import os

step_function = json.loads(os.environ['STEP_FUNCTION_ARN']) 
bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')

def lambda_handler(event, context):
    stepfunctions = boto3.client('stepfunctions')
    # query = event.get('query')
    # filters = event.get('filters')
    # Extract parameters from the API Gateway request
    body = json.loads(event.get('body', '{}'))
    query = body.get('query')
    
    filters = body.get('filters')
    # object_name = body.get('object_name')
    
    # if not object_name:
    #     return {
    #         'statusCode': 400,
    #         'body': json.dumps({'error': 'object_name is required'}),
    #         'headers': {
    #             'Content-Type': 'application/json'
    #         }
    #     }
    
    # Generate a unique job ID
    job_id = str(uuid.uuid4())
    validation_prompt = (
        f"The user query is: '{query}'. We are building a Q&A bot to analyze feedback from the Hospital Employee Data survey. "
        "The survey contains multiple columns discussing various aspects of employees, such as sex, gender, employee ID, name, location, "
        "ethnicity, comments, views, sentiments, departments, centers of excellence (COE), COE level departments, tenure bands. "
        "A query is valid if it relates to any of these demographics or survey data points, such as analyzing feedback, employee sentiments, "
        "or identifying trends and insights based on the survey results. This includes queries that focus on specific locations or other demographics."
        
        "Valid queries are those that ask for analysis of feedback, employee sentiment (positive or negative), key trends, or insights from the survey data. "
        "Even if phrased differently, queries that seek information about feedback or analysis related to specific locations or demographics should be considered valid. "
        "For instance, 'What is the feedback from employees in a certain location?', 'Can you provide overall feedback?', or 'What are the most common comments?' are all valid."
    
        "Examples of valid queries include: "
        "'What are the top insights from the survey?', 'What are the major areas of positive feedback?', "
        "'What is the overall employee sentiment?', or 'What are the most common negative comments?'. "
        "Queries related to feedback for specific locations or groups are also valid."
    
        "Invalid queries are those unrelated to the survey's demographics or feedback data. This includes questions that ask about "
        "topics outside the survey, such as external statistics or company policies. For example, queries like 'What is the weather today?' "
        "or 'What is the company's financial performance?' would be considered invalid."
    
        "To summarize: If the query asks for feedback, insights, or analysis related to the survey data, respond with 'Valid'. "
        "If it asks for unrelated information, respond with 'Invalid'. The response must be a single word: 'Valid' or 'Invalid'."
    )
    # validation_prompt = (
    #     f"The user query is: '{query}'. This bot analyzes feedback from the Hospital Employee Data survey. "
    #     "Valid queries ask for information related to employee feedback, sentiments, trends, or insights from the survey data, "
    #     "including demographics like sex, gender, location, departments, and tenure."
        
    #     "Examples of valid queries include: 'What is the employee feedback?', 'What are the top insights?', "
    #     "'What are the major areas of positive feedback?', or 'What is the overall sentiment?' "
    #     "Questions about specific demographics, such as feedback for certain locations or departments, are also valid."
    
    #     "Invalid queries are those unrelated to the survey, such as questions about external data or topics like the weather or financial performance."
        
    #     "If the query is about feedback, insights, or analysis related to the survey, respond with 'Valid'. "
    #     "If it asks for unrelated information, respond with 'Invalid'. The response must be a single word: 'Valid' or 'Invalid'."
    # )


    print(validation_prompt)
    # Invoke the Bedrock model to validate the query
    validation_result = invoke_bedrock_model(validation_prompt, model_id="anthropic.claude-3-5-sonnet-20240620-v1:0")
    print("In the result :",validation_result)

    # If the query is invalid, return an error response
    if validation_result == "Invalid":
        return {
            'statusCode': 400,
            'body': {'error': 'Invalid query. Please ask about the Advocate Health Employee survey.'},
            'headers': {
                'Content-Type': 'application/json'
            }
        }
    
    # Define the input for the Step Function
    input_data = {
        "query": query,
        "filters": filters
        # "object_name": object_name
    }
    
    # Start Step Function execution
    try:
        response = stepfunctions.start_execution(
            stateMachineArn=step_function,
            name=f"processing-job-{job_id}",
            input=json.dumps(input_data)
        )
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
            'headers': {
                'Content-Type': 'application/json'
            }
        }
    
    # Return the job ID to the frontend
    return {
        'statusCode': 200,
        'body': json.dumps({
            'job_id': job_id,
            'execution_arn': response['executionArn']
        }),
        # 'headers': {
        #     'Content-Type': 'application/json'
        # }
    }

def invoke_bedrock_model(prompt, model_id):
    native_request = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4000,
        "temperature": 0.5,
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            }
        ],
    }
    request = json.dumps(native_request)
    try:
        response = bedrock_client.invoke_model(modelId=model_id, body=request)
        model_response = json.loads(response["body"].read())
        response_text = model_response["content"][0]["text"]
        return response_text.strip()
    except (ClientError, Exception) as e:
        raise Exception(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
