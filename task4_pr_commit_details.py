"""
Task-4: Extract PR commit details and save as CSV.

From pr_commit_details table create a CSV with:
- PRID: pr_id
- PRSHA: sha
- PRCOMMITMESSAGE: message (cleaned)
- PRFILE: filename
- PRSTATUS: status
- PRADDS: additions
- PRDELSS: deletions
- PRCHANGECOUNT: changes
- PRDIFF: patch (special characters removed to avoid encoding issues)
"""

import os
import re
import pandas as pd


def clean_special_chars(text):
    """Remove special characters that might cause encoding issues."""
    if pd.isna(text):
        return ""
    text = str(text)
    # Remove non-ASCII characters and control characters
    text = re.sub(r"[^\x00-\x7F]+", " ", text)
    text = re.sub(r"[\x00-\x1F\x7F-\x9F]", " ", text)
    # Normalize whitespace
    text = " ".join(text.split())
    return text.strip()


def main():
    print("Loading pr_commit_details table from Hugging Face...")
    src = "hf://datasets/hao-li/AIDev/pr_commit_details.parquet"
    df = pd.read_parquet(src)

    required_cols = [
        "pr_id",
        "sha",
        "message",
        "filename",
        "status",
        "additions",
        "deletions",
        "changes",
        "patch",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in dataset: {missing}")

    # Build output DataFrame with required headers
    out = pd.DataFrame({
        "PRID": df["pr_id"],
        "PRSHA": df["sha"],
        "PRCOMMITMESSAGE": df["message"].apply(clean_special_chars),
        "PRFILE": df["filename"],
        "PRSTATUS": df["status"],
        "PRADDS": df["additions"],
        "PRDELSS": df["deletions"],
        "PRCHANGECOUNT": df["changes"],
        "PRDIFF": df["patch"].apply(clean_special_chars),
    })

    os.makedirs("data", exist_ok=True)
    out_path = os.path.join("data", "task4_pr_commit_details.csv")
    print(f"Saving Task-4 CSV to {out_path} ...")
    out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"✅ Task 4 complete! Total records: {len(out)}")


if __name__ == "__main__":
    main()
