"""
Task-1: Extract selected fields from the AIDev all_pull_request table
and save them as a CSV with specific headers.
"""

import os
import re
import pandas as pd


def is_english(text):
    """Return True if text does NOT contain Chinese characters."""
    if pd.isna(text):
        return True
    return not bool(re.search(r"[\u4e00-\u9fff]", str(text)))


def clean_body(text):
    """Flatten markdown into one safe line."""
    if pd.isna(text):
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def main():
    src = "hf://datasets/hao-li/AIDev/all_pull_request.parquet"
    print("Loading all_pull_request table...")
    df = pd.read_parquet(src)

    # Required columns
    cols = ["title", "id", "agent", "body", "repo_id", "repo_url"]
    df = df[cols].copy()

    # Filter: Keep only English rows
    mask = df["title"].map(is_english) & df["body"].map(is_english)
    df = df[mask]

    # Fix numeric IDs -> strings without .0
    for col in ["id", "repo_id"]:
        df[col] = (
            pd.to_numeric(df[col], errors="coerce")
            .astype("Int64")
            .astype("string")
            .fillna("")
        )

    # Flatten body text
    df["body"] = df["body"].apply(clean_body)

    # Rename columns
    df.rename(
        columns={
            "title": "TITLE",
            "id": "ID",
            "agent": "AGENTNAME",
            "body": "BODYSTRING",
            "repo_id": "REPOID",
            "repo_url": "REPOURL",
        },
        inplace=True,
    )

    # Ensure output directory exists
    os.makedirs("data", exist_ok=True)

    output_path = os.path.join("data", "task1_all_pull_requests.csv")

    print(f"Saving Task-1 CSV to {output_path} ...")
    df.to_csv(output_path, index=False)
    print("✅ Task 1 CSV successfully generated.")


if __name__ == "__main__":
    main()
