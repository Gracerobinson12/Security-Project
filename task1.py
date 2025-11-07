"""
Task-1: Extract selected fields from the AIDev all_pull_request table
and save them as a CSV with specific headers.
"""

import os
import pandas as pd


def main():
    source_path = "hf://datasets/hao-li/AIDev/all_pull_request.parquet"
    print("Loading all_pull_request table from Hugging Face...")
    all_pr_df = pd.read_parquet(source_path)

    required_cols = ["title", "id", "agent", "body", "repo_id", "repo_url"]
    subset_df = all_pr_df[required_cols].copy()

    rename_map = {
        "title": "TITLE",
        "id": "ID",
        "agent": "AGENTNAME",
        "body": "BODYSTRING",
        "repo_id": "REPOID",
        "repo_url": "REPOURL",
    }
    subset_df.rename(columns=rename_map, inplace=True)

    os.makedirs("data", exist_ok=True)
    output_path = os.path.join("data", "task1_all_pull_requests.csv")

    print(f"Saving Task-1 CSV to {output_path} ...")
    subset_df.to_csv(output_path, index=False)
    print("Done. CSV successfully generated.")


if __name__ == "__main__":
    main()
