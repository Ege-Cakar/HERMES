import argparse
import os
import yaml
import sys

# Add project root to PYTHONPATH
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT)

from src.io_utils import read_jsonl, ensure_dir, save_parquet


def main(cfg):
    input_path = cfg["data"]["input_jsonl"]
    out_dir = cfg["data"]["out_dir"]

    print(f"Reading JSONL: {input_path}")
    pairs = read_jsonl(input_path)

    records = []
    for ex in pairs:
        nl = ex.get("nl_text", "").strip()
        lean = ex.get("lean_text", "").strip()

        # Remove empty or corrupted entries
        if len(nl) == 0 or len(lean) == 0:
            continue

        records.append({
            "id": ex.get("id", ""),
            "nl_len": len(nl),
            "lean_len": len(lean),
        })

    # Save metadata
    ensure_dir(out_dir)
    meta_path = os.path.join(out_dir, "pairs_meta.parquet")
    save_parquet(records, meta_path)

    print(f"✅ Wrote metadata: {meta_path}")
    print(f"✅ Total valid pairs: {len(records)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="project_config.yaml")
    args = ap.parse_args()

    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)

    main(cfg)
