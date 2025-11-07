"""
Task-2: Extract selected fields from the AIDev all_repository table
and save them as a CSV with specific headers.

Outputs:
    data/task2_all_repository.csv
"""

import os
import pandas as pd


def main():
    # Path to the all_repository table on Hugging Face
    source_path = "hf://datasets/hao-li/AIDev/all_repository.parquet"

    print("Loading all_repository table from Hugging Face...")
    repo_df = pd.read_parquet(source_path)

    # Select the required columns
    required_cols = ["id", "language", "stars", "url"]

    missing = [c for c in required_cols if c not in repo_df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    subset_df = repo_df[required_cols].copy()

    # Rename for the final CSV
    rename_map = {
        "id": "REPOID",
        "language": "LANG",
        "stars": "STARS",
        "url": "REPOURL",
    }
    subset_df.rename(columns=rename_map, inplace=True)

    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)

    output_path = os.path.join("data", "task2_all_repository.csv")

    print(f"Saving Task-2 CSV to {output_path} ...")
    subset_df.to_csv(output_path, index=False)
    print("✅ Done. CSV successfully generated.")


if __name__ == "__main__":
    main()
