import json
import os
import requests
import pyarrow.parquet as pq
from io import BytesIO

DATASET = "hoskinson-center/proofnet"
CONFIG = "plain_text"
SPLITS = ["validation", "test"]

BASE_LISTING = f"https://huggingface.co/api/datasets/{DATASET}/parquet/{CONFIG}"


def list_parquet_urls(split):
    url = f"{BASE_LISTING}/{split}"
    r = requests.get(url)
    r.raise_for_status()
    # Returns list of full URLs to parquet files
    return r.json()


def download_parquet(url):
    print(f"Downloading {url}")
    r = requests.get(url)
    r.raise_for_status()
    return BytesIO(r.content)


def main():
    os.makedirs("data", exist_ok=True)
    out_path = "data/pairs.jsonl"

    valid, dropped = 0, 0

    with open(out_path, "w") as out:
        for split in SPLITS:
            urls = list_parquet_urls(split)

            for parquet_url in urls:
                buffer = download_parquet(parquet_url)

                table = pq.read_table(buffer)
                df = table.to_pandas()

                print("Columns:", df.columns.tolist())
                print(df.head(3))

                for _, row in df.iterrows():
                    nl = str(row.get("proof_nl", "")).strip()
                    lean = str(row.get("proof_fl", "")).strip()

                    if not nl or not lean:
                        dropped += 1
                        continue

                    rec = {
                        "id": row.get("id", f"{split}_{valid}"),
                        "nl_text": nl,
                        "lean_text": lean,
                    }

                    out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    valid += 1

    print("=====================================")
    print(f"✅ Saved {valid} examples to {out_path}")
    print(f"🚫 Dropped {dropped} empty/missing examples")
    print("=====================================")


if __name__ == "__main__":
    main()
