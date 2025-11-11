# scripts/prepare_dataset.py

import os
import json
import pandas as pd

IN_PATH = "data/pairs.jsonl"
OUT_DIR = "outputs/meta"
OUT_META = f"{OUT_DIR}/pairs_meta.parquet"


def read_jsonl(path):
    with open(path) as f:
        for line in f:
            yield json.loads(line)


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def main():
    print("Loading pairs from:", IN_PATH)

    records = []
    for ex in read_jsonl(IN_PATH):
        records.append({
            "id": ex["id"],
            "nl_len": len(ex["nl_text"]),
            "lean_len": len(ex["lean_text"]),
            "split": ex["split"]
        })

    df = pd.DataFrame(records)

    ensure_dir(OUT_DIR)
    df.to_parquet(OUT_META)

    print("=====================================")
    print(f"✅ Metadata saved to {OUT_META}")
    print(f"Total examples: {len(df)}")
    print("=====================================")


if __name__ == "__main__":
    main()
