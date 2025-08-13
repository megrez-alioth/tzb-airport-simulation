#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

def analyze_5am_data():
    """分析5点时段的数据"""
    # 读取数据
    file_path = '数据/5月航班运行数据（脱敏）.xlsx'
    data = pd.read_excel(file_path)
    print(f'原始数据总记录数: {len(data)}')
    print(f'列名: {list(data.columns)}')

    # 筛选ZGGG起飞数据
    if '计划起飞站四字码' in data.columns:
        zggg_data = data[data['计划起飞站四字码'] == 'ZGGG'].copy()
    else:
        print("找不到起飞机场列")
        return
    print(f'ZGGG起飞航班: {len(zggg_data)} 班')

    # 处理时间数据
    for col in ['计划离港时间', '实际离港时间', '实际起飞时间']:
        if col in zggg_data.columns:
            zggg_data[col] = pd.to_datetime(zggg_data[col], errors='coerce')

    # 处理缺失的起飞时间：用离港时间+20分钟估算
    missing_takeoff = zggg_data['实际起飞时间'].isna()
    if missing_takeoff.sum() > 0:
        print(f"缺失实际起飞时间的航班: {missing_takeoff.sum()} 班")
        # 对于有离港时间但没有起飞时间的，用离港时间+20分钟估算
        zggg_data.loc[missing_takeoff & zggg_data['实际离港时间'].notna(), '实际起飞时间'] = (
            zggg_data.loc[missing_takeoff & zggg_data['实际离港时间'].notna(), '实际离港时间'] + 
            pd.Timedelta(minutes=20)
        )

    # 筛选有效数据
    valid_data = zggg_data[
        zggg_data['计划离港时间'].notna() & 
        zggg_data['实际起飞时间'].notna()
    ].copy()

    # 计算延误
    valid_data['起飞延误分钟'] = (
        valid_data['实际起飞时间'] - valid_data['计划离港时间']
    ).dt.total_seconds() / 60

    # 分析5点时段的数据
    five_am_data = valid_data[valid_data['计划离港时间'].dt.hour == 5]
    print(f'\n=== 5点时段数据分析 ===')
    print(f'5点航班总数: {len(five_am_data)}')

    if len(five_am_data) > 0:
        print(f'平均延误: {five_am_data["起飞延误分钟"].mean():.1f}分钟')
        print(f'延误分布:')
        
        delays = five_am_data['起飞延误分钟']
        print(f'  <= 60分钟: {(delays <= 60).sum()} 班 ({(delays <= 60).sum()/len(delays)*100:.1f}%)')
        print(f'  > 120分钟: {(delays > 120).sum()} 班 ({(delays > 120).sum()/len(delays)*100:.1f}%)')
        
        print(f'\n按日期分布:')
        for date in sorted(five_am_data['计划离港时间'].dt.date.unique()):
            day_data = five_am_data[five_am_data['计划离港时间'].dt.date == date]
            avg_delay = day_data['起飞延误分钟'].mean()
            severe_count = (day_data['起飞延误分钟'] > 120).sum()
            normal_count = (day_data['起飞延误分钟'] <= 60).sum()
            print(f'  {date}: {len(day_data)}班, 平均延误{avg_delay:.1f}分钟, 正常延误{normal_count}班, 严重延误{severe_count}班')

        # 分析为什么没有被识别为特殊情况
        print(f'\n=== 分析特殊处理逻辑 ===')
        for date in sorted(five_am_data['计划离港时间'].dt.date.unique()):
            day_data = five_am_data[five_am_data['计划离港时间'].dt.date == date]
            if len(day_data) == 0:
                continue
                
            delays = day_data['起飞延误分钟']
            normal_delays = (delays <= 60).sum()  # 正常延误（<=60分钟）
            severe_delays = (delays > 120).sum()  # 严重延误（>120分钟）
            
            # 当前的特殊处理条件
            condition1 = len(day_data) >= 3  # 至少3班航班
            condition2 = severe_delays <= 2  # 严重延误不超过2班
            condition3 = normal_delays / len(day_data) > 0.6 if len(day_data) > 0 else False  # 60%以上航班延误正常
            
            print(f'  {date}: 航班{len(day_data)}班, 严重延误{severe_delays}班, 正常延误{normal_delays}班')
            print(f'    条件1(>=3班): {condition1}, 条件2(严重延误<=2班): {condition2}, 条件3(正常延误>60%): {condition3}')
            print(f'    是否满足特殊处理: {condition1 and condition2 and condition3}')

if __name__ == "__main__":
    analyze_5am_data()
