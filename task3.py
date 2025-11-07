"""
Task-3: Extract selected fields from the AIDev pr_task_type table
and save them as a CSV with specific headers.

Outputs:
    data/task3_pr_task_type.csv
"""

import os
import pandas as pd


def main():
    source_path = "hf://datasets/hao-li/AIDev/pr_task_type.parquet"

    print("Loading pr_task_type table from Hugging Face...")
    pr_task_df = pd.read_parquet(source_path)

    # Required columns
    required_cols = ["id", "title", "reason", "type", "confidence"]
    missing = [c for c in required_cols if c not in pr_task_df.columns]
    if missing:
        raise ValueError(f"Missing columns in dataset: {missing}")

    subset_df = pr_task_df[required_cols].copy()

    # Rename to match deliverable headers
    rename_map = {
        "id": "PRID",
        "title": "PRTITLE",
        "reason": "PRREASON",
        "type": "PRTYPE",
        "confidence": "CONFIDENCE",
    }
    subset_df.rename(columns=rename_map, inplace=True)

    os.makedirs("data", exist_ok=True)
    output_path = os.path.join("data", "task3_pr_task_type.csv")

    print(f"Saving Task-3 CSV to {output_path} ...")
    subset_df.to_csv(output_path, index=False)
    print("✅ Done. CSV successfully generated.")


if __name__ == "__main__":
    main()
