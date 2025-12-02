import pandas as pd
import os
from security_keywords import SECURITY_KEYWORDS

def check_security_keywords(text):
    """Check if text contains any security keywords"""
    if pd.isna(text):
        return 0
    text_lower = str(text).lower()
    return 1 if any(keyword in text_lower for keyword in SECURITY_KEYWORDS) else 0

# Load required data
print("Loading data for Task 5...")
try:
    task1_df = pd.read_csv('data/task1_all_pull_requests.csv')
    task3_df = pd.read_csv('data/task3_pr_task_type.csv')
except FileNotFoundError as e:
    print(f"Error: Required input file not found - {e}")
    exit(1)

# Merge data on ID/PRID
print("Merging datasets...")
merged_df = pd.merge(
    task1_df[['ID', 'AGENTNAME', 'TITLE', 'BODYSTRING']],
    task3_df[['PRID', 'PRTYPE', 'CONFIDENCE']],
    left_on='ID',
    right_on='PRID',
    how='inner'
)

if merged_df.empty:
    print("Warning: No matching records found between Task 1 and Task 3 data!")

# Apply security keyword check
print("Analyzing security keywords...")
merged_df['SECURITY'] = merged_df.apply(
    lambda row: check_security_keywords(str(row['TITLE']) + ' ' + str(row['BODYSTRING'])),
    axis=1
)

# Create final output
output_df = pd.DataFrame({
    'ID': merged_df['ID'],
    'AGENT': merged_df['AGENTNAME'],
    'TYPE': merged_df['PRTYPE'],
    'CONFIDENCE': merged_df['CONFIDENCE'],
    'SECURITY': merged_df['SECURITY']
})

# Save to CSV in data folder
output_file = 'data/task5_security_flagged.csv'
output_df.to_csv(output_file, index=False)

print(f"\n✓ Task 5 complete! Output saved to: {output_file}")
print(f"Total records: {len(output_df)}")
print(f"Security-flagged PRs: {output_df['SECURITY'].sum()}")
print(f"Percentage flagged: {output_df['SECURITY'].sum() / len(output_df) * 100:.2f}%")