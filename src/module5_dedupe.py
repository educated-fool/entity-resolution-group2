import random
import numpy as np
import pandas as pd
import re
import dedupe
from dedupe import RecordLink

def dedupe_pipeline(left_df, right_df, filepath):
    # Preprocess DataFrames
    right_df.rename(columns={'zip_code': 'postal_code'}, inplace=True)
    #    random.seed(10201)
    #    np.random.seed(10201)
    def preprocess_data(df):
        df['postal_code'] = df['postal_code'].astype(str).str.split('-').str[0].str.strip()
        df['state'] = df['state'].astype(str).str.strip()
        df['name'] = df['name'].astype(str).str.lower().str.strip().apply(lambda x: re.sub(r'[^\w\s]', '', x))
        df['address'] = df['address'].astype(str).str.lower().str.strip().apply(lambda x: re.sub(r'[^\w\s]', '', x))
        df['city'] = df['city'].astype(str).str.lower().str.strip().apply(lambda x: re.sub(r'[^\w\s]', '', x))
        return df
    
    left_df = preprocess_data(left_df)
    right_df = preprocess_data(right_df)
    
    left_df.fillna('unknown', inplace=True)
    left_df['postal_code'] = left_df['postal_code'].astype(str).apply(lambda x: x.split('.')[0])
    
    # Convert data to dictionaries
    data_1 = {i: row.to_dict() for i, row in left_df.iterrows()}
    data_2 = {i: row.to_dict() for i, row in right_df.iterrows()}
    
    # Set up dedupe
    fields = [{'field': field, 'type': 'String'} for field in ['name', 'address', 'city', 'state', 'postal_code']]
    linker = RecordLink(fields)
    
    # Prepare training
    linker.prepare_training(data_1, data_2)
    dedupe.console_label(linker)
    linker.train()
    
    # Match records
    results = linker.join(data_1, data_2, threshold=0.8)
    submission = [(index1, index2, score) for (index1, index2), score in results]
    
    # Create DataFrame
    df = pd.DataFrame(submission, columns=['left_dataset', 'right_dataset', 'confidence_score'])
    df.to_csv(filepath, index=False)
    return df