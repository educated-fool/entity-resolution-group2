from thefuzz import fuzz, process
from multiprocessing import Pool
import pandas as pd
import numpy as np
import re

def thefuzz_pipeline(left_df, right_df, output_csv):
    # Fill missing values
    left_df['address'] = left_df['address'].fillna('')
    left_df['postal_code'] = left_df['postal_code'].astype(str).fillna('00000')

    def preprocess_data(df):
        df['state'] = df['state'].astype(str).str.strip()
        df['name'] = df['name'].astype(str).str.lower().str.strip().apply(lambda x: re.sub(r'[^\w\s]', '', x))
        df['address'] = df['address'].astype(str).str.lower().str.strip().apply(lambda x: re.sub(r'[^\w\s]', '', x))
        df['city'] = df['city'].astype(str).str.lower().str.strip().apply(lambda x: re.sub(r'[^\w\s]', '', x))
        return df
    
    # Apply the preprocessing function to both dataframes
    left_df = preprocess_data(left_df)
    right_df = preprocess_data(right_df)

    #deal with postercode problem
    left_df['postal_code'] = left_df['postal_code'].astype(str).apply(lambda x: x.split('.')[0])
    right_df['zip_code'] = right_df['zip_code'].astype(str).apply(lambda x: x.split('-')[0].zfill(5))

    # Create combined fields
    left_df['combined'] = left_df['name'] + ' ' + left_df['address'] + ' ' + left_df['state'] + ' ' + left_df['postal_code'].astype(str)
    right_df['combined'] = right_df['name'] + ' ' + right_df['address'] + ' ' + right_df['state'] + ' ' + right_df['zip_code'].astype(str)

    # Create blocking keys
    left_df['block_key'] = left_df['name'].str[0] + left_df['state']
    right_df['block_key'] = right_df['name'].str[0] + right_df['state']

    # Create dictionaries for each block
    right_dict = right_df.groupby('block_key')['combined'].apply(list).to_dict()

   # Matching function to find best matches above score of 80
    def match_entries(left_entries, right_entries):
        matches = []
        for left_entry in left_entries:
            best_match = process.extractOne(left_entry, right_entries, scorer=process.fuzz.partial_ratio, score_cutoff=80)
            if best_match:
                matches.append((left_entry, best_match[0], best_match[1]))
        return matches

    # Perform matching within blocks
    results = []
    for key in left_df['block_key'].unique():
        if key in right_dict:
            block_matches = match_entries(left_df[left_df['block_key'] == key]['combined'], right_dict[key])
            results.extend(block_matches)

    # Map combined strings back to IDs
    left_id_map = left_df.set_index('combined')['entity_id'].to_dict()
    right_id_map = right_df.set_index('combined')['business_id'].to_dict()
    
    # Extract entity_id, business_id, and confidence score
    match_data = []
    for left_combined, right_combined, score in results:
        entity_id = left_id_map.get(left_combined, None)
        business_id = right_id_map.get(right_combined, None)
        if entity_id and business_id:
            match_data.append({'left_dataset': entity_id, 'right_dataset': business_id, 'confidence_score': score/100})

    # Convert to DataFrame
    thefuzz_submission = pd.DataFrame(match_data)
    
    # Write results to CSV
    thefuzz_submission.to_csv(output_csv, index=False)

    return thefuzz_submission