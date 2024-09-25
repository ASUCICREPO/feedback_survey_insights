import boto3
import os
import json
import uuid
from botocore.exceptions import ClientError
import time


bucket = os.environ['BUCKET_NAME']
athena_table_name = os.environ['ATHENA_TABLE']
athena_database =  os.environ['ATHENA_DATABASE']
COMMENT_COLUMNS = os.environ['COMMENT_COLUMNS']

s3 = boto3.client('s3')
bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')
sagemaker = boto3.client('sagemaker')

def lambda_handler(event, context):
   
    query = event.get('query')
    filters = event.get('filters', {})
    
    sql_query = generate_sql_query(filters)
    try:
        
        
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
    ATHENA_OUTPUT_BUCKET = f"s3://{bucket}/filter/"  # S3 bucket where Athena will put the results
    # s3://samplecdksurveyfeedbacktesting3/filter/
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
        # print(result_data)
        
        # Check if the result has less than or equal to 2 rows
        if len(result_data['ResultSet']['Rows']) <= 2:
            raise ValueError("The filters you selected have no data. Please select different filters to get insights.")
        
        

        # Get list of objects
        s3_client = boto3.client('s3')
    
        # Get list of objects
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix='filter/')
        
        # Sort objects by last modified date
        objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
        print("The objects are",objects)
        
        # Get latest object
        latest_file = objects[0]
        print("this is file",latest_file['Key'])
        if latest_file['Key'][-8:] == "metadata":
            latest_file = objects[1]
        # print(latest_file['Key'])
        
        # Access latest file data
        file_name = latest_file['Key'].split('/')[-1]
        
        return file_name
    else:
        return None
      
    
def generate_sql_query(filters):
    # Define the columns to always select
    
    # Base SQL query with selected comment columns
    sql_query = f"SELECT * FROM {athena_table_name}"
    
    # Check if there are filters to apply
    if filters:
        where_conditions = []
        # Build the WHERE conditions based on the filters
        for filter_category in filters:
            for category, values in filter_category.items():
                if isinstance(values, list):
                    # Multiple values for the same category, use IN clause
                    where_conditions.append(f"{category} IN ({', '.join([f'\'{v}\'' for v in values])})")
                else:
                    # Single value, use equality
                    where_conditions.append(f"{category} = '{values}'")
    
        # Combine all WHERE conditions with AND
        sql_query += " WHERE " + " AND ".join(where_conditions)

    # Ensure the query follows S3 Select validation rules
    sql_query += ";"

    return sql_query.strip()