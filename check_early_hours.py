#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查凌晨1-5点的详细数据
"""

import pandas as pd
import numpy as np

def check_early_hours():
    # 读取数据
    df = pd.read_excel('/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/5月航班运行数据（脱敏）.xlsx')
    zggg_flights = df[df['计划起飞站四字码'] == 'ZGGG'].copy()
    
    # 时间处理
    time_cols = ['计划离港时间', '实际离港时间', '实际起飞时间']
    for col in time_cols:
        if col in zggg_flights.columns:
            zggg_flights[col] = pd.to_datetime(zggg_flights[col], errors='coerce')
    
    # 处理缺失的起飞时间
    missing_takeoff = zggg_flights['实际起飞时间'].isna()
    zggg_flights.loc[missing_takeoff & zggg_flights['实际离港时间'].notna(), '实际起飞时间'] = (
        zggg_flights.loc[missing_takeoff & zggg_flights['实际离港时间'].notna(), '实际离港时间'] + 
        pd.Timedelta(minutes=20)
    )
    
    # 计算延误
    zggg_flights['起飞延误分钟'] = (
        zggg_flights['实际起飞时间'] - zggg_flights['计划离港时间']
    ).dt.total_seconds() / 60
    
    # 过滤有效数据
    valid_data = zggg_flights[
        (zggg_flights['起飞延误分钟'] >= -60) &
        (zggg_flights['起飞延误分钟'] <= 600) &
        zggg_flights['起飞延误分钟'].notna()
    ].copy()
    
    print("=== 凌晨1-5点数据分析 ===\n")
    
    # 分析1-5点每个小时的情况
    for hour in range(1, 6):
        hour_data = valid_data[valid_data['计划离港时间'].dt.hour == hour]
        
        if len(hour_data) > 0:
            avg_delay = hour_data['起飞延误分钟'].mean()
            severe_delays = (hour_data['起飞延误分钟'] > 120).sum()
            severe_ratio = severe_delays / len(hour_data) if len(hour_data) > 0 else 0
            
            print(f"凌晨{hour:02d}:00时段:")
            print(f"  航班数量: {len(hour_data)} 班")
            print(f"  平均延误: {avg_delay:.1f} 分钟")
            print(f"  严重延误航班: {severe_delays} 班")
            print(f"  严重延误比例: {severe_ratio:.1%}")
            
            # 显示延误分布
            delay_ranges = {
                '0-30分钟': ((hour_data['起飞延误分钟'] >= 0) & (hour_data['起飞延误分钟'] <= 30)).sum(),
                '30-60分钟': ((hour_data['起飞延误分钟'] > 30) & (hour_data['起飞延误分钟'] <= 60)).sum(),
                '60-120分钟': ((hour_data['起飞延误分钟'] > 60) & (hour_data['起飞延误分钟'] <= 120)).sum(),
                '120-240分钟': ((hour_data['起飞延误分钟'] > 120) & (hour_data['起飞延误分钟'] <= 240)).sum(),
                '>240分钟': (hour_data['起飞延误分钟'] > 240).sum()
            }
            
            print("  延误分布:")
            for range_name, count in delay_ranges.items():
                ratio = count / len(hour_data) * 100 if len(hour_data) > 0 else 0
                print(f"    {range_name}: {count}班 ({ratio:.1f}%)")
            
            # 判定是否应该被识别为系统性问题
            should_be_problematic = (avg_delay > 100 or severe_ratio > 0.4) and len(hour_data) >= 5
            print(f"  应识别为系统性问题: {'是' if should_be_problematic else '否'}")
            print()
        else:
            print(f"凌晨{hour:02d}:00时段: 无航班\n")

if __name__ == "__main__":
    check_early_hours()
