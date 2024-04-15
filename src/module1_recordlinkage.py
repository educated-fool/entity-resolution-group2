import pandas as pd
import recordlinkage
from recordlinkage.preprocessing import clean
from recordlinkage import Compare

def record_linkage_pipeline(left_df, right_df, filename, threshold=0.85):
    # Preprocess dataframes
    left_df['name'] = clean(left_df['name'])
    left_df['address'] = clean(left_df['address'])
    left_df['city'] = clean(left_df['city'])
    left_df['state'] = clean(left_df['state'])
    right_df['name'] = clean(right_df['name'])
    right_df['address'] = clean(right_df['address'])
    right_df['city'] = clean(right_df['city'])
    right_df['state'] = clean(right_df['state'])

    left_df['postal_code'] = left_df['postal_code'].astype(str).apply(lambda x: x.split('.')[0])
    right_df['zip_code'] = right_df['zip_code'].astype(str)
    right_df['zip_code'] = right_df['zip_code'].apply(lambda x: x.split('-')[0].zfill(5))
    left_df.fillna('unknown', inplace=True)
    right_df.rename(columns={'zip_code': 'postal_code'}, inplace=True)

    # Compute candidate links
    left_df['block_key'] = left_df['address'].str[0] + left_df['name'].str[0] + left_df['state'] + left_df['postal_code'].str[:3]
    right_df['block_key'] = right_df['address'].str[0] + right_df['name'].str[0] + right_df['state'] + right_df['postal_code'].str[:3]
    indexer = recordlinkage.Index()
    indexer.block('block_key')
    candidate_links = indexer.index(left_df, right_df)

    # Compute comparison vectors
    compare_cl = Compare()
    columns_to_compare = ['name', 'address', 'city', 'state', 'postal_code']
    for column in columns_to_compare:
        compare_cl.string(column, column, method='jarowinkler', threshold=threshold)
    features = compare_cl.compute(candidate_links, left_df, right_df)

    # Compute scores and export csv
    weights = [1, 1, 0.2, 0.2, 0.2]
    scores = features.dot(weights) / sum(weights)
    matches_df = pd.DataFrame({'left_index': features.index.get_level_values(0), 'right_index': features.index.get_level_values(1), 'score': scores})
    high_confidence_matches = matches_df[matches_df['score'] >= 0.8]
    result = pd.merge(high_confidence_matches, left_df, left_on='left_index', right_index=True)
    result = pd.merge(result, right_df, left_on='right_index', right_index=True, suffixes=('_left', '_right'))
    recordlinkage_submission = result[['entity_id', 'business_id', 'score']].copy()
    recordlinkage_submission.columns = ['left_dataset', 'right_dataset', 'confidence_score']
    recordlinkage_submission['confidence_score'] = recordlinkage_submission['confidence_score'].round(2)
    recordlinkage_submission.to_csv(filename, index=False)

    return recordlinkage_submission