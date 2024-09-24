import boto3
import os
import json
import uuid
from botocore.exceptions import ClientError
import time


bucket = json.loads(os.environ['BUCKET_NAME']) 
Athena_table_name = json.loads(os.environ['ATHENA_TABLE'])
athena_database =  json.loads(os.environ['ATHENA_DATABASE'])
COMMENT_COLUMNS = json.loads(os.environ['COMMENT_COLUMNS'])

s3 = boto3.client('s3')
bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')
sagemaker = boto3.client('sagemaker')

def lambda_handler(event, context):
    # Get the S3 bucket and file key from the event
    # bucket_name = event['Records'][0]['s3']['bucket']['name']
    # object_key = event['Records'][0]['s3']['object']['key']
    
    # Check if the uploaded file is the CSV we want to process
    # if object_key.endswith('.csv'):
    
    query = event.get('query')
    filters = event.get('filters', {})
    
    sql_query = generate_sql_query(filters)
    try:
        # User query from the event
        # body = json.loads(event.get("body", ""))
        # user_query = body.get("query", "")
        # filters = body.get('filters', [])
        
        
        # Generate SQL query using NLP based on user query
        sql_query = generate_sql_query(filters)
        print(f"The SQL Query is: {sql_query}")
        
        # Query CSV data
        # filtered_data = query_csv_s3(s3, BUCKET_NAME, object_key, sql_query, use_header=True)
        object_name = athena_query(sql_query)
        
        # Processing job name
        job_id = str(uuid.uuid4())
        # processing_job_name = f'processing-job-{job_id}'
    
        
        return {
            
            'job_id': job_id,
            'query': query,
            'filters': filters,
            'object_name':object_name
        }
        
        # response_body = {
        #      "insights": validated_insights['insights'],
        #      "recommendations":validated_insights['recommendations'],
        #      "presigned_url":presigned_url,
        #      "overall_summary": summary
        # }
        
        # response_body = {
        #       "filter":filters,
        #       "user_query":user_query,
        #       "sql_query":sql_query
        # }
    
        # return {
        # 'statusCode': 200,
        # 'body': response_body
        # }
        
        
        # return {
        #     'statusCode': 200,
        #     # 'message': f"Data fetched based on the query: '{user_query}'",
        #     # 'data': filtered_data,
        #     'insights': validated_insights,
        #     # 'summaries': summaries_details,
        #     'summary': summarized_data
        # }
    except ClientError as e:
        return {
            'statusCode': 400,
            'error': f"Client error: {e}"
        }
        
    except ValueError as e:
        # Handle specific validation errors like no data
        raise ValueError(f"The filters you selected have no data. Please select different filters to get insights.")
        
    except Exception as e:
        return {
            'statusCode': 500,
            'error': f"Unexpected error: {e}"
        }


def athena_query(query):
    s3 = boto3.client('s3')
    client = boto3.client('athena')
    ATHENA_OUTPUT_BUCKET = f"s3://{bucket}/filtered/"  # S3 bucket where Athena will put the results
    DATABASE = athena_database  # The name of the database in Athena
    QUERY = query  # The SQL query you want to execute
    response = client.start_query_execution(
        QueryString=QUERY,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': ATHENA_OUTPUT_BUCKET
        }
    )
    print(response)

    query_execution_id = response['QueryExecutionId']
    
    while True:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']
        
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:  # (optional) checking the status 
            break
        
        time.sleep(3)  # Poll every 5 seconds
    
    # Here, you can handle the response as per your requirement
    if state == 'SUCCEEDED':
        # Fetch the results if necessary
        # print(result_data)
        
        result_data = client.get_query_results(QueryExecutionId=query_execution_id)
        
        # Check if the result has less than or equal to 2 rows
        if len(result_data['ResultSet']['Rows']) <= 2:
            raise ValueError("The filters you selected have no data. Please select different filters to get insights.")
        
        

        # Get list of objects
        s3_client = boto3.client('s3')
    
        # Get list of objects
        response = s3_client.list_objects_v2(Bucket='sampledataemployeesurvey', Prefix='results/')
        
        # Sort objects by last modified date
        objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
        
        # Get latest object
        latest_file = objects[0]
        print("this is file",latest_file['Key'])
        if latest_file['Key'][-8:] == "metadata":
            latest_file = objects[1]
        # print(latest_file['Key'])
        
        # Access latest file data
        object_name = latest_file['Key']
        
        
        
        return object_name[8:]
    else:
        return None
      
    
def generate_sql_query(filters):
    # Define the columns to always select
    comment_columns = [
        "Comment: Reason to Stay",
        "Comment: Reason to Leave",
        "Comment: Well-Being at Work",
        "Comment: Well-Being Outside Work",
        "Comment: Burnout Reason",
        "Comment: Burnout Improvement",
        "Comment: What is important for us to know?"
    ]
    
    # Base SQL query with selected comment columns
    sql_query = "SELECT * FROM survey_data"
    
    # Check if there are filters to apply
    if filters:
        where_conditions = []
        # Build the WHERE conditions based on the filters
        for filter_category in filters:
            for category, values in filter_category.items():
                if isinstance(values, list):
                    # Multiple values for the same category, use IN clause
                    where_conditions.append(f"{category} IN ({', '.join([f'\'{v}\'' for v in values])})")
                    # where_conditions.append(f"{category} IN ({', '.join([f'\'{v}\'' for v in values])})")
                else:
                    # Single value, use equality
                    where_conditions.append(f"{category} = '{values}'")
    
        # Combine all WHERE conditions with AND
        sql_query += " WHERE " + " AND ".join(where_conditions)

    # Ensure the query follows S3 Select validation rules
    sql_query += ";"

    return sql_query.strip()