import pandas as pd
import os

SOURCE_PATH = "data/raw_source/bureau.csv"
TARGET_PATH = "data/lake/bureau.parquet"

def convert_to_parquet():
    if not os.path.exists(SOURCE_PATH):
        print(f"Error: {SOURCE_PATH} not found!")
        return

    os.makedirs("data/lake", exist_ok=True)
    print("Converting bureau dataset to Parquet format...")
    
    # قراءة وتحويل الملف لـ Parquet مضغوط بـ snappy
    df = pd.read_csv(SOURCE_PATH)
    df.to_parquet(TARGET_PATH, engine="pyarrow", compression="snappy", index=False)
    
    print(f"Successfully created Parquet Data Lake file at: {TARGET_PATH}")

if __name__ == "__main__":
    convert_to_parquet()
