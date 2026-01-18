import pandas as pd
import numpy as np
import os

def split_csv(file_path, num_files=10):
    # 1. 加载数据
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"错误：找不到文件 {file_path}")
        return

    # 2. 打散数据 (Shuffle)
    # frac=1 表示抽取 100% 的数据，random_state 保证结果可复现（如果需要随机可以去掉）
    df_shuffled = df.sample(frac=1, random_state=42).reset_index(drop=True)

    # 3. 计算每个文件的行数
    total_rows = len(df_shuffled)
    rows_per_file = int(np.ceil(total_rows / num_files))

    # 创建存储目录（可选）
    output_dir = "split_files"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 4. 切分并保存
    for i in range(num_files):
        start_idx = i * rows_per_file
        end_idx = (i + 1) * rows_per_file
        
        # 提取切片
        chunk = df_shuffled.iloc[start_idx:end_idx]
        
        # 命名规则：cosmetics_part_1.csv, cosmetics_part_2.csv ...
        output_name = os.path.join(output_dir, f"cosmetics_part_{i+1}.csv")
        
        # 保存，不保留索引
        chunk.to_csv(output_name, index=False, encoding='utf-8')
        print(f"已生成: {output_name} (包含 {len(chunk)} 行)")

if __name__ == "__main__":
    split_csv('data/cosmetics.csv')