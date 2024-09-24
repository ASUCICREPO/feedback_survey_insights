import argparse
import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
from sentence_transformers import SentenceTransformer
import logging

def main(input_data, output_data, object_name):
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--input-data', type=str)
    # parser.add_argument('--output-data', type=str)
    # parser.add_argument('--object-name', type=str)
    # args = parser.parse_args()
    
    # input_data_path = args.input_data
    # output_data_path = args.output_data
    # object_name = args.object_name

    # Input file path
    input_file = os.path.join(input_data, object_name)
    logging.info(f"Input file path: {input_file}")
    
    # Read CSV data
    data = pd.read_csv(input_file)

    # List of comment columns
    # comment_columns = [
    #     'Comment: Reason to Stay',
    #     'Comment: Reason to Leave',
    #     'Comment: Well-Being at Work',
    #     'Comment: Well-Being Outside Work',
    #     'Comment: Burnout Reason',
    #     'Comment: Burnout Improvement',
    #     'Comment: What is important for us to know?'
    # ]

    comment_columns = [
        'comment_reason_to_stay',
        'comment_reason_to_leave',
        'comment_well_being_at_work',
        'comment_well_being_outside_work',
        'comment_burnout_reason',
        'comment_burnout_improvement',
        'comment_what_is_important_for_us_to_know'
    ]
    
    # Fill NaN values and combine comments
    data[comment_columns] = data[comment_columns].fillna('')
    # data['combined_comments'] = data[comment_columns].agg(' '.join, axis=1)

    # Remove rows with empty combined comments
    # data = data[data['combined_comments'].str.strip() != '']
    data.reset_index(drop=True, inplace=True)
    
    # Load pre-trained model
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    # Compute embeddings
    documents = data[comment_columns].agg(' '.join, axis=1).tolist()
    embeddings = model.encode(documents, show_progress_bar=True)
    
    # Normalize embeddings
    scaler = StandardScaler()
    embeddings_scaled = scaler.fit_transform(embeddings)
    
    # Perform DBSCAN clustering
    dbscan = DBSCAN(eps=0.5, min_samples=2, metric='cosine')
    clusters = dbscan.fit_predict(embeddings_scaled)
    
    # Add cluster labels to data
    data['cluster'] = clusters
    
    # Identify unique comments
    data['is_unique'] = data['cluster'] == -1
    
    # Output file path
    output_csv = os.path.join(output_data, 'clustered_results.csv')
    
    # Save the data with cluster labels
    data.to_csv(output_csv, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process employee survey data.")
    parser.add_argument('--input-data', type=str, required=True, help="Path to input data directory.")
    parser.add_argument('--output-data', type=str, required=True, help="Path to output data directory.")
    parser.add_argument('--object-name', type=str, required=True, help="Name of the input file to process.")
    # parser.add_argument('--job-id', type=str, required=True, help="Job ID for naming output files.")
    args = parser.parse_args()

    main(args.input_data, args.output_data, args.object_name)