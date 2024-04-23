import pandas as pd
import fnmatch
import textdistance

def fnmatch_textdistance_pipeline(left_df, right_df, filename, threshold = 0.8):
    # Clean & Standardize Data
    left_df['postal_code'] = left_df['postal_code'].fillna(0).astype(int).astype(str)
    right_df['zip_code'] = right_df['zip_code'].str.slice(0, 5)
    left_df = left_df.astype(str)
    right_df = right_df.astype(str)

    def clean_dataframe(df):
        def clean_text_column(column):
            column = column.str.lower() 
            column = column.str.replace(r'[^\w\s]', '', regex=True) 
            column = column.str.strip() 
            return column
        for column in df.columns:
            if df[column].dtype == 'object':
                df[column] = clean_text_column(df[column])
        return df
    left_df = clean_dataframe(left_df)
    right_df = clean_dataframe(right_df)

    # Create Blocking Keys
    left_df['block_key'] = (left_df['state'] + " " +
                        left_df['address'].str[0] + " " +  
                        left_df['name'].str[0] + " " +  
                        left_df['postal_code'].str[:3])  
    right_df['block_key'] = (right_df['state'] + " " +
                         right_df['address'].str[0] + " " +  
                         right_df['name'].str[0] + " " +  
                         right_df['zip_code'].str[:3])  
    
    # Blocking Dataset
    blocked_pairs = pd.merge(left_df, right_df, on='block_key', suffixes=('_left', '_right'))

    # Apply fnmatch for Matching and textdistance for Similarity Score
    def calculate_confidence(name_left, name_right):
        if name_left == name_right:
            return 1.0
        elif fnmatch.fnmatch(name_left, f"*{name_right}*") or fnmatch.fnmatch(name_right, f"*{name_left}*"):
            similarity = textdistance.jaro_winkler(name_left, name_right)
            return min(similarity * 1.2, 1.0)
        else:
            similarity = textdistance.jaccard(name_left, name_right)
            return similarity
        
    blocked_pairs['confidence_score'] = blocked_pairs.apply(
        lambda row: calculate_confidence(row['name_left'].lower(), row['name_right'].lower()), axis=1
    )

    high_confidence_matches = blocked_pairs[blocked_pairs['confidence_score'] > threshold]
    
    # Write the Result to a CSV
    high_confidence_matches = high_confidence_matches.rename(columns={'entity_id': 'left_dataset', 'business_id': 'right_dataset'})
    output_columns = ['left_dataset', 'right_dataset', 'confidence_score']
    high_confidence_matches[output_columns].to_csv(filename, index=False)

    return high_confidence_matches[output_columns]