import pandas as pd
import numpy as np
import os

def split_csv(file_path, num_files=10):
    # 1. Load data
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: File not found {file_path}")
        return

    # 2. Shuffle data
    # frac=1 means sampling 100% of data, random_state ensures reproducibility (remove if random results needed)
    df_shuffled = df.sample(frac=1, random_state=42).reset_index(drop=True)

    # 3. Calculate rows per file
    total_rows = len(df_shuffled)
    rows_per_file = int(np.ceil(total_rows / num_files))

    # Create output directory (optional)
    output_dir = "split_files"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 4. Split and save
    for i in range(num_files):
        start_idx = i * rows_per_file
        end_idx = (i + 1) * rows_per_file
        
        # Extract slice
        chunk = df_shuffled.iloc[start_idx:end_idx]
        
        # Naming convention: cosmetics_part_1.csv, cosmetics_part_2.csv ...
        output_name = os.path.join(output_dir, f"cosmetics_part_{i+1}.csv")
        
        # Save without preserving index
        chunk.to_csv(output_name, index=False, encoding='utf-8')
        print(f"Generated: {output_name} (contains {len(chunk)} rows)")

if __name__ == "__main__":
    split_csv('data/cosmetics.csv')