import json
import boto3
import pandas as pd
import numpy as np  # Import NumPy
from io import StringIO
from botocore.exceptions import ClientError
import os

bucket = json.loads(os.environ['BUCKET_NAME']) 
bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')
COMMENT_COLUMNS = json.loads(os.environ['COMMENT_COLUMNS'])

def invoke_bedrock_model(prompt, model_id):
    # model_id = "anthropic.claude-3-haiku-20240307-v1:0"
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
        
        # Log the model response for debugging
        # print(f"Model Response: {model_response}")
        
        response_text = model_response["content"][0]["text"]
        return response_text.strip()
    except (ClientError, Exception) as e:
        raise Exception(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")

def lambda_handler(event, context):
    try:
        # Define S3 bucket and key
        query = event.get('query')
        bucket_name = bucket
        key = "processed/clustered_results.csv"
        
        if not bucket_name or not key:
            return {
                'statusCode': 400,
                'body': json.dumps('Error: Both bucket_name and key are required.')
            }
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Get the CSV file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        # Read CSV content into pandas DataFrame
        df = pd.read_csv(StringIO(content))
        print("DataFrame Head:\n", df.head())
        
        # Check the number of data rows (excluding headers)
        data_row_count = len(df)
        if data_row_count == 0 or data_row_count == 2:
            return {
                'statusCode': 400,
                'body': json.dumps('Error: The CSV file contains insufficient data (either empty or duplicate columns). Please change your filters.')
            }
        
        # Initialize list to store desired rows
        result_list = []
        
        # Filter rows where 'is_unique' is True
        if 'is_unique' in df.columns:
            unique_rows = df[df['is_unique'] == True].drop(columns=['combined_comments'], errors='ignore').to_dict(orient='records')
            result_list.extend(unique_rows)
            print(f"Unique Rows Added: {len(unique_rows)}")
        else:
            return {
                'statusCode': 400,
                'body': json.dumps("Error: 'is_unique' column not found in the CSV.")
            }
        
        # Handle 'cluster' column
        if 'cluster' in df.columns:
            # Ensure 'cluster' values are numeric and non-negative
            clusters = df['cluster'].dropna().unique()
            clusters = [c for c in clusters if isinstance(c, (int, float, np.integer, np.floating)) and c >= 0]
            print("Filtered Clusters:", clusters)
            
            for cluster_value in clusters:
                # Select the first row for each unique cluster value
                cluster_row = df[df['cluster'] == cluster_value].iloc[0].drop(labels=['combined_comments'], errors='ignore').to_dict()
                result_list.append(cluster_row)
                print(f"Added Cluster Row for Cluster {cluster_value}: ID {cluster_row.get('id', 'N/A')}")
        else:
            return {
                'statusCode': 400,
                'body': json.dumps("Error: 'cluster' column not found in the CSV.")
            }
        
        # Remove potential duplicates if a row is both 'is_unique' and part of a 'cluster'
        # This ensures each row appears only once in the result
        unique_result = {tuple(item.items()): item for item in result_list}.values()
        final_result = list(unique_result)
        print("Final Result List Length:", len(final_result))
        
        if not final_result:
            return {
                'statusCode': 400,
                'body': json.dumps('Error: No data available after filtering.')
            }
        
        # Prepare data for prompt by selecting relevant comment fields
        # Assuming 'comment_reason_to_stay', 'comment_reason_to_leave', etc., are the comment fields
        # Adjust the fields as per your actual column names
        comment_fields = [
            'comment_reason_to_stay',
            'comment_reason_to_leave',
            'comment_well_being_at_work',
            'comment_well_being_outside_work',
            'comment_burnout_reason',
            'comment_burnout_improvement',
            'comment_what_is_important_for_us_to_know'
        ]
        
        comments_list = []
        for row in final_result:
            # comments = [row.get(field, "") for field in comment_fields if row.get(field, "")]
            # combined_comments = " ".join(comments)
            comments_list.append(row)
        
        # Construct the prompt
        prompt = (
            "We have a large dataset of employee survey comments that have been processed using clustering techniques. "
            "Each cluster represents a distinct theme or insight derived from the data. To streamline the analysis and reduce token usage, "
            "we have selected one representative comment from each cluster. Below are the top rows from each cluster:\n\n"
        )
        
        for idx, comment in enumerate(comments_list, start=1):
            prompt += f"Cluster {idx}:\n- {comment}\n\n"
        
        prompt += (
            f"In response to the user query: '{query}', please generate detailed insights and actionable recommendations based on the comments provided. "
            "Each insight should be thoroughly explained with context, covering the key analysis and underlying factors."
            "For each insight, also provide a detailed recommendation that addresses the identified issue, opportunity, or pattern. "
            "The recommendation should offer concrete solutions or next steps. Additionally, include a entire sample row that exemplifies each insight. "
            "Ensure the output is in JSON format with the following structure:\n\n"
            "{\n"
            '  "insights": [\n'
            "    {\n"
            '      "insight": "Insight description",\n'
            '      "recommendation": "Actionable recommendation",\n'
            '      "sample_row": "A entire row that illustrates the insight"\n'
            "    },\n"
            "    ...\n"
            "  ],\n"
            '  "summary": "Overall summary of the insights."\n'
            "}\n\n"
            "Please ensure the JSON strictly follows the above format to facilitate parsing on the frontend."
        )
        
        print("Constructed Prompt:\n", prompt)
        
        # Invoke the Bedrock model
        model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"  # Replace with your actual model ID
        llm_response = invoke_bedrock_model(prompt, model_id)
        print("LLM Response:\n", llm_response)
        
        # Validate and parse the JSON response from LLM
        try:
            insights_summary = json.loads(llm_response)
        except json.JSONDecodeError:
            return {
                'statusCode': 500,
                'body': json.dumps('Error: The model response is not valid JSON.')
            }
        
        # Optionally, validate the structure of the JSON
        if not isinstance(insights_summary, dict) or 'insights' not in insights_summary or 'summary' not in insights_summary:
            return {
                'statusCode': 500,
                'body': json.dumps('Error: The model response does not follow the expected JSON structure.')
            }
        
        return {
            'statusCode': 200,
            'body': insights_summary
        }
    
    except pd.errors.EmptyDataError:
        # Specific handling for empty CSV files
        return {
            'statusCode': 400,
            'body': json.dumps('Error: The CSV file is empty. Please provide a valid file with data.')
        }
    except Exception as e:
        # Handle unexpected errors
        return {
            'statusCode': 500,
            'body': json.dumps(f'An unexpected error occurred: {str(e)}')
        }
