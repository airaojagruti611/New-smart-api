"""
convert_parquet.py
──────────────────
Utility script to convert Parquet data lake files into client-friendly CSV or Excel files.

Usage Examples:
  1) Convert all data_lake files to CSV & Excel:
     python convert_parquet.py

  2) Convert specific data_lake path to CSV only:
     python convert_parquet.py --input data_lake --format csv --output client_data_csv

  3) Export to a single Excel workbook with sheets:
     python convert_parquet.py --input data_lake --format excel --output client_report.xlsx
"""

import argparse
import os
from pathlib import Path
import pandas as pd


def find_parquet_files(input_path: Path):
    """Find all .parquet files recursively inside input_path."""
    if input_path.is_file() and input_path.suffix == ".parquet":
        return [input_path]
    elif input_path.is_dir():
        return sorted(list(input_path.rglob("*.parquet")))
    return []


def group_parquet_files(files):
    """Group parquet files by stream/symbol or folder path."""
    groups = {}
    for f in files:
        parent_name = f.parent.name
        grandparent_name = f.parent.parent.name if f.parent.parent else ""
        
        if "stream=" in grandparent_name or "stream=" in parent_name:
            group_key = f"{grandparent_name}/{parent_name}"
        else:
            group_key = parent_name
            
        groups.setdefault(group_key, []).append(f)
    return groups


def export_to_csv(groups, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n[+] Exporting CSV files to folder: {output_dir.resolve()}")
    
    for group_name, files in groups.items():
        dfs = []
        for f in files:
            try:
                df = pd.read_parquet(f)
                dfs.append(df)
            except Exception as e:
                print(f"    [!] Error reading {f.name}: {e}")
        
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            safe_name = group_name.replace("=", "_").replace("/", "_").replace("\\", "_")
            out_file = output_dir / f"{safe_name}.csv"
            combined_df.to_csv(out_file, index=False)
            print(f"    [✔] Saved CSV ({len(combined_df)} rows): {out_file.name}")


def export_to_excel(groups, output_excel: Path):
    output_excel.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n[+] Exporting Excel workbook to: {output_excel.resolve()}")
    
    try:
        import openpyxl
    except ImportError:
        print("[!] Note: 'openpyxl' is required for Excel export. Install with: pip install openpyxl")
        print("[!] Falling back to CSV export...")
        export_to_csv(groups, output_excel.parent / "converted_csv_output")
        return

    with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
        sheet_count = 0
        for group_name, files in groups.items():
            dfs = []
            for f in files:
                try:
                    df = pd.read_parquet(f)
                    dfs.append(df)
                except Exception as e:
                    print(f"    [!] Error reading {f.name}: {e}")
            
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)
                # Excel sheet name max length is 31 chars
                sheet_name = group_name.replace("stream=", "").replace("dt=", "").replace("/", "_")[-31:]
                combined_df.to_excel(writer, sheet_name=sheet_name, index=False)
                sheet_count += 1
                print(f"    [✔] Added Sheet '{sheet_name}' ({len(combined_df)} rows)")
        
        if sheet_count > 0:
            print(f"\n[✔] Excel workbook created successfully: {output_excel.resolve()}")


def main():
    parser = argparse.ArgumentParser(description="Convert Parquet data lake files to CSV or Excel")
    parser.add_argument("--input", "-i", type=str, default="data_lake", help="Path to data_lake directory or parquet file")
    parser.add_argument("--format", "-f", choices=["csv", "excel", "both"], default="both", help="Output format: csv, excel, or both")
    parser.add_argument("--output", "-o", type=str, default="converted_output", help="Output directory or Excel filename")

    args = parser.parse_args()
    input_path = Path(args.input)

    if not input_path.exists():
        print(f"[!] Specified input path '{input_path}' does not exist.")
        if Path("angel_md_data_lake").exists():
            print("[+] Found 'angel_md_data_lake', using it as input path...")
            input_path = Path("angel_md_data_lake")
        else:
            print("[!] Please provide a valid path using --input <path_to_data_lake>")
            return

    parquet_files = find_parquet_files(input_path)
    if not parquet_files:
        print(f"[!] No .parquet files found under {input_path}")
        return

    print(f"[+] Found {len(parquet_files)} .parquet file(s) under {input_path}")
    groups = group_parquet_files(parquet_files)

    out_path = Path(args.output)

    if args.format in ["csv", "both"]:
        csv_dir = out_path if args.format == "csv" else out_path / "csv"
        export_to_csv(groups, csv_dir)

    if args.format in ["excel", "both"]:
        excel_file = out_path if out_path.suffix == ".xlsx" else (out_path if args.format == "excel" else out_path) / "market_data_report.xlsx"
        export_to_excel(groups, excel_file)


if __name__ == "__main__":
    main()
