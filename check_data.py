import pandas as pd
import os
import sys
from pathlib import Path

# Ensure dq_framework is in path
sys.path.append(str(Path(__file__).parent))

from dq_framework.loader import DataLoader

data_path = "/home/sanmi/Documents/HS2/HS2_PROJECTS_2025/AIMS_LOCAL/data/Samples_LH_Bronze_Aims_26_parquet"

if not os.path.exists(data_path):
    print(f"Data path not found: {data_path}")
    sys.exit(1)

files = [f for f in os.listdir(data_path) if f.endswith('.parquet')]
print(f"Found {len(files)} parquet files to check.")

for f in files:
    file_path = os.path.join(data_path, f)
    try:
        # Use DataLoader for memory optimization and large file handling
        # It will auto-detect large files and sample if necessary
        df = DataLoader.load_data(file_path)
        
        if 'OWNERID' in df.columns:
            print(f"Found OWNERID in {f}")
            nulls = df['OWNERID'].isna().sum()
            total = len(df)
            print(f"Total rows: {total}")
            print(f"Nulls in OWNERID: {nulls}")
            print(f"Null percent: {nulls/total*100}")
    except Exception as e:
        print(f"Error processing {f}: {e}")

