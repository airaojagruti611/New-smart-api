import os
import re
import ast
import csv
from datetime import datetime

# Input and output paths
LOG_FILE = r"d:\New-smart-api\angel_md_redis\logs\2026-07-15\bidask_analyzer.log"
CSV_FILE = r"d:\New-smart-api\angel_md_redis\logs\2026-07-15\bidask_signals.csv"
XLSX_FILE = r"d:\New-smart-api\angel_md_redis\logs\2026-07-15\bidask_signals.xlsx"

def parse_logs():
    if not os.path.exists(LOG_FILE):
        print(f"Log file not found at: {LOG_FILE}")
        return

    records = []
    # Regex to capture timestamp and payload
    pattern = re.compile(r"^([\d\-:\s]+) \| INFO \| bidask_analyzer \| EMIT.*payload=(\{.*\})")

    with open(LOG_FILE, "r") as f:
        for line in f:
            match = pattern.search(line)
            if match:
                log_time_str = match.group(1).strip()
                payload_str = match.group(2).strip()
                try:
                    payload = ast.literal_eval(payload_str)
                    # Convert ts_ms to human readable time if desired
                    ts_ms = payload.get("ts_ms")
                    if ts_ms:
                        try:
                            # Convert millisecond epoch to readable timestamp
                            dt = datetime.fromtimestamp(int(ts_ms) / 1000.0)
                            payload["timestamp_formatted"] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        except Exception:
                            payload["timestamp_formatted"] = ""
                    else:
                        payload["timestamp_formatted"] = ""
                    
                    payload["log_time"] = log_time_str
                    records.append(payload)
                except Exception as e:
                    pass

    if not records:
        print("No EMIT logs found to parse.")
        return

    # Define fields
    headers = [
        "log_time",
        "timestamp_formatted",
        "key",
        "kind",
        "bid",
        "ask",
        "raw_spread",
        "spread_pct",
        "mid",
        "depth",
        "liquidity_score",
        "signal",
        "spread_ratio"
    ]

    # Save to CSV
    try:
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(records)
        print(f"Successfully saved {len(records)} entries to CSV: {CSV_FILE}")
    except Exception as e:
        print(f"Failed to write CSV: {e}")

    # Try saving to Excel if pandas and openpyxl are available
    try:
        import pandas as pd
        df = pd.DataFrame(records)
        # Reorder columns
        existing_cols = [c for c in headers if c in df.columns]
        df = df[existing_cols]
        df.to_excel(XLSX_FILE, index=False)
        print(f"Successfully saved entries to Excel: {XLSX_FILE}")
    except ImportError:
        print("Pandas/Openpyxl not installed. Skipping XLSX export. Run 'pip install pandas openpyxl' to enable it.")
    except Exception as e:
        print(f"Failed to write XLSX: {e}")

if __name__ == "__main__":
    parse_logs()
