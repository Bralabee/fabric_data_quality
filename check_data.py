import pandas as pd
import os

data_path = "/home/sanmi/Documents/HS2/HS2_PROJECTS_2025/AIMS_LOCAL/data/Samples_LH_Bronze_Aims_26_parquet"

files = [f for f in os.listdir(data_path) if f.endswith('.parquet')]
for f in files:
    try:
        df = pd.read_parquet(os.path.join(data_path, f))
        if 'OWNERID' in df.columns:
            print(f"Found OWNERID in {f}")
            nulls = df['OWNERID'].isna().sum()
            total = len(df)
            print(f"Total rows: {total}")
            print(f"Nulls in OWNERID: {nulls}")
            print(f"Null percent: {nulls/total*100}")
    except Exception as e:
        pass

