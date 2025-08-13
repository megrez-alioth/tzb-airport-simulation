#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试数据格式
"""

import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data.data_loader import FlightDataLoader

# 数据文件路径
current_dir = os.path.dirname(os.path.abspath(__file__))
data_file = os.path.join(os.path.dirname(current_dir), '数据', '5月航班运行数据（实际数据列）.xlsx')

print("=== 数据调试信息 ===")

# 加载数据
loader = FlightDataLoader(data_file)
raw_data = loader.load_raw_data()

print(f"原始数据行数: {len(raw_data)}")
print(f"原始数据列: {list(raw_data.columns)}")
print()

# 提取ZGGG数据
zggg_data = loader.extract_zggg_data(raw_data)
print(f"ZGGG数据行数: {len(zggg_data)}")

# 显示前几行ZGGG数据
print("\n=== ZGGG数据样本 ===")
if len(zggg_data) > 0:
    print(zggg_data.head(3).to_string())
    
    print("\n=== 时间列数据样本 ===")
    time_cols = ['实际起飞时间', '实际落地时间']
    available_time_cols = [col for col in time_cols if col in zggg_data.columns]
    
    if available_time_cols:
        for col in available_time_cols:
            print(f"{col}:")
            print(zggg_data[col].head(3).to_string())
            print(f"数据类型: {zggg_data[col].dtype}")
            print(f"非空值: {zggg_data[col].notna().sum()}/{len(zggg_data)}")
            print()
    else:
        print("没有找到时间列")

    print("\n=== 机场代码列数据样本 ===")  
    airport_cols = ['实际起飞站四字码', '实际到达站四字码']
    available_airport_cols = [col for col in airport_cols if col in zggg_data.columns]
    
    for col in available_airport_cols:
        print(f"{col}:")
        print(f"唯一值数量: {zggg_data[col].nunique()}")
        print(f"前5个值: {zggg_data[col].value_counts().head()}")
        print()
