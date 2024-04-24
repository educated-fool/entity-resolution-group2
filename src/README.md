# README: Splink Advanced Data Linking Module Exploration

## Overview
This README outlines the key elements of our exploration with the Splink Advanced Data Linking Techniques, as part of our ongoing effort to enhance data linkage capabilities in our projects. The primary focus of this module, authored by Riley Xiong, was to employ advanced data cleaning, blocking key creation, matching, scoring, and model training techniques. Unfortunately, the exploration did not proceed to completion, resulting in no Python scripts under the `src` directory.

## Components
### 1. Data Cleaning
- **Objective**: Standardize postal codes and cleanse text fields (e.g., names, addresses, and city names) to ensure uniformity across datasets.
- **Outcome**: Achieved uniform data which facilitates more accurate comparisons.

### 2. Blocking Keys Creation
- **Objective**: Implement strategic blocking keys by extracting initial characters of address, name, state, and postal code to enhance processing efficiency.
- **Outcome**: Reduced computational load by narrowing down potential comparisons to similar entries.

### 3. Matching and Scoring
- **Objective**: Use a combination of deterministic rules and probabilistic scoring with measures such as Levenshtein, Jaro-Winkler, and Jaccard indexes.
- **Outcome**: Prioritized entries with higher similarity scores within the blocked groups.

### 4. Model Training and Prediction
- **Objective**: Conduct Expectation Maximization to refine match probabilities with a low threshold (0.10) to capture a broad range of potential matches.
- **Outcome**: Generated a large number of comparisons, exceeding processing capabilities, which hindered efficient assessment.

### 5. Output
- **Issue**: The system struggled to process and confirm high-confidence matches efficiently due to the excessive number of comparisons.
- **Future Work**: Adjustments to blocking and scoring settings are necessary to manage computational demands and improve output efficacy.

## Explanation for Missing Python Files
Due to the unanticipated computational demands and the subsequent inability to run effective tests and generate outputs, the exploration phase did not produce deployable Python scripts. Therefore, no `.py` files are present under the `src` directory. Future iterations will focus on optimizing the processes to achieve manageable and successful outputs.

## Conclusion
This exploration into advanced data linking techniques highlighted significant potential and challenges. Adjustments and optimizations are planned for upcoming versions to better handle the complexities of large-scale data linkage.