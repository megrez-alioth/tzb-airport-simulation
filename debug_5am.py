#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试早上5点数据，看看为什么没有被识别为系统性问题时段
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def debug_5am_data():
    """调试早上5点的数据"""
    # 读取数据
    df = pd.read_excel('/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/5月航班运行数据（脱敏）.xlsx')
    
    # 筛选ZGGG起飞航班
    zggg_flights = df[df['计划起飞站四字码'] == 'ZGGG'].copy()
    
    # 数据清理
    valid_data = zggg_flights.dropna(subset=['航班号', '计划离港时间']).copy()
    
    # 时间格式转换
    time_cols = ['计划离港时间', '实际离港时间', '实际起飞时间']
    for col in time_cols:
        if col in valid_data.columns:
            valid_data[col] = pd.to_datetime(valid_data[col], errors='coerce')
    
    # 处理缺失的起飞时间
    missing_takeoff = valid_data['实际起飞时间'].isna()
    if missing_takeoff.sum() > 0:
        valid_data.loc[missing_takeoff & valid_data['实际离港时间'].notna(), '实际起飞时间'] = (
            valid_data.loc[missing_takeoff & valid_data['实际离港时间'].notna(), '实际离港时间'] + 
            pd.Timedelta(minutes=20)
        )
    
    # 计算起飞延误
    valid_data['起飞延误分钟'] = (
        valid_data['实际起飞时间'] - valid_data['计划离港时间']
    ).dt.total_seconds() / 60
    
    # 过滤异常数据
    valid_data = valid_data[
        (valid_data['起飞延误分钟'] >= -60) &
        (valid_data['起飞延误分钟'] <= 600)
    ]
    
    print("=== 早上5点数据调试 ===")
    
    # 筛选早上5点的数据
    hour_5_data = valid_data[valid_data['计划离港时间'].dt.hour == 5]
    
    print(f"早上5点总航班数: {len(hour_5_data)}")
    
    if len(hour_5_data) > 0:
        delays = hour_5_data['起飞延误分钟']
        
        avg_delay = delays.mean()
        severe_delays_120 = (delays > 120).sum()  # 严重延误 >2小时
        severe_ratio_120 = severe_delays_120 / len(delays)
        
        delays_20 = (delays > 20).sum()  # 延误 >20分钟
        ratio_20 = delays_20 / len(delays)
        
        print(f"平均延误: {avg_delay:.1f} 分钟")
        print(f"总航班数: {len(delays)} 班")
        print(f"延误>120分钟的航班数: {severe_delays_120} 班")
        print(f"严重延误比例(>120分钟): {severe_ratio_120:.1%}")
        print(f"延误>20分钟的航班数: {delays_20} 班") 
        print(f"延误比例(>20分钟): {ratio_20:.1%}")
        
        print(f"\n系统性问题判定条件检查:")
        print(f"1. 平均延误>240分钟: {avg_delay > 240} ({avg_delay:.1f} > 240)")
        print(f"2. 严重延误比例>60%: {severe_ratio_120 > 0.6} ({severe_ratio_120:.1%} > 60%)")
        print(f"3. 样本数量>=10班: {len(delays) >= 10} ({len(delays)} >= 10)")
        
        # 显示延误分布
        print(f"\n延误时间分布:")
        delay_ranges = [
            ("提前", lambda x: x < 0),
            ("0-20分钟", lambda x: (x >= 0) & (x <= 20)),
            ("20-60分钟", lambda x: (x > 20) & (x <= 60)),
            ("60-120分钟", lambda x: (x > 60) & (x <= 120)),
            ("120-240分钟", lambda x: (x > 120) & (x <= 240)),
            (">240分钟", lambda x: x > 240)
        ]
        
        for label, condition in delay_ranges:
            count = condition(delays).sum()
            pct = count / len(delays) * 100
            print(f"  {label}: {count} 班 ({pct:.1f}%)")
            
        print(f"\n早上5点具体航班延误情况:")
        for idx, row in hour_5_data.iterrows():
            delay = row['起飞延误分钟']
            date = row['计划离港时间'].strftime('%m-%d')
            time = row['计划离港时间'].strftime('%H:%M')
            print(f"  {date} {time}: 延误 {delay:.0f} 分钟")
    else:
        print("没有早上5点的数据")

if __name__ == "__main__":
    debug_5am_data()
