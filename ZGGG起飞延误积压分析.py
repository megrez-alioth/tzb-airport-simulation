#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG起飞延误积压分析
专门分析ZGGG起飞航班的延误情况和积压时段识别
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

def load_and_analyze_data():
    """载入数据并分析字段结构"""
    try:
        df = pd.read_excel('/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/5月航班运行数据（脱敏）.xlsx')
        print("数据载入成功!")
        print(f"总记录数: {len(df)}")
        print("\n=== 数据字段分析 ===")
        print("字段列表:")
        for i, col in enumerate(df.columns):
            print(f"{i+1}. {col}")
        
        print(f"\n数据形状: {df.shape}")
        print("\n=== 前5行数据预览 ===")
        print(df.head())
        
        return df
    except Exception as e:
        print(f"数据载入失败: {e}")
        return None

def extract_zggg_departures(df):
    """提取ZGGG起飞航班"""
    # 提取ZGGG起飞航班 - 使用正确的字段名
    zggg_dep = df[df['实际起飞站四字码'] == 'ZGGG'].copy()
    print(f"\n=== ZGGG起飞航班统计 ===")
    print(f"ZGGG起飞航班总数: {len(zggg_dep)}")
    
    # 分析关键时间字段
    time_fields = ['计划离港时间', '实际离港时间', '实际起飞时间', '原计划离港时间']
    
    print(f"\n=== 时间字段分析 ===")
    for field in time_fields:
        if field in zggg_dep.columns:
            non_null = zggg_dep[field].notna().sum()
            print(f"{field}: {non_null}/{len(zggg_dep)} ({non_null/len(zggg_dep)*100:.1f}%)")
            if non_null > 0:
                print(f"  示例: {zggg_dep[field].dropna().iloc[0]}")
    
    # 分析其他相关字段
    other_fields = ['调时航班标识', '取消时间']
    print(f"\n=== 其他字段分析 ===")
    for field in other_fields:
        if field in zggg_dep.columns:
            non_null = zggg_dep[field].notna().sum()
            print(f"{field}: {non_null}/{len(zggg_dep)} ({non_null/len(zggg_dep)*100:.1f}%)")
            if non_null > 0:
                unique_vals = zggg_dep[field].dropna().unique()
                print(f"  取值: {unique_vals[:10]}")  # 显示前10个值
    
    return zggg_dep

def analyze_time_relationships(zggg_dep):
    """分析时间字段之间的关系"""
    print(f"\n=== 时间字段关系分析 ===")
    
    # 转换时间字段
    time_fields = ['计划离港时间', '实际离港时间', '实际起飞时间', '原计划离港时间']
    for field in time_fields:
        if field in zggg_dep.columns:
            zggg_dep[field] = pd.to_datetime(zggg_dep[field], errors='coerce')
    
    # 计算有效数据
    valid_data = zggg_dep.dropna(subset=['计划离港时间', '实际离港时间', '实际起飞时间'])
    print(f"有完整时间数据的航班: {len(valid_data)}")
    
    if len(valid_data) > 0:
        # 计算时间差
        valid_data = valid_data.copy()
        valid_data['离港延误'] = (valid_data['实际离港时间'] - valid_data['计划离港时间']).dt.total_seconds() / 60
        valid_data['起飞延误'] = (valid_data['实际起飞时间'] - valid_data['计划离港时间']).dt.total_seconds() / 60
        valid_data['离港到起飞间隔'] = (valid_data['实际起飞时间'] - valid_data['实际离港时间']).dt.total_seconds() / 60
        
        print(f"\n=== 时间差统计 (分钟) ===")
        print(f"离港延误: 平均 {valid_data['离港延误'].mean():.1f}, 中位数 {valid_data['离港延误'].median():.1f}")
        print(f"起飞延误: 平均 {valid_data['起飞延误'].mean():.1f}, 中位数 {valid_data['起飞延误'].median():.1f}")
        print(f"离港到起飞间隔: 平均 {valid_data['离港到起飞间隔'].mean():.1f}, 中位数 {valid_data['离港到起飞间隔'].median():.1f}")
        
        # 分析离港到起飞间隔的分布
        interval_stats = valid_data['离港到起飞间隔'].describe()
        print(f"\n=== 离港到起飞间隔分布 ===")
        print(interval_stats)
        
        # 分析异常情况
        long_intervals = valid_data[valid_data['离港到起飞间隔'] > 60]  # 超过1小时
        print(f"\n离港到起飞间隔超过1小时的航班: {len(long_intervals)}")
        
        negative_intervals = valid_data[valid_data['离港到起飞间隔'] < 0]
        print(f"离港到起飞间隔为负数的航班: {len(negative_intervals)}")
        
        return valid_data
    
    return None

def propose_delay_criteria(valid_data):
    """提出延误判定标准"""
    print(f"\n=== 延误判定标准分析 ===")
    
    if valid_data is None or len(valid_data) == 0:
        return None
    
    # 方案1: 基于计划离港时间的起飞延误
    criterion1 = valid_data['起飞延误'] > 15  # 起飞延误超过15分钟
    delayed1 = valid_data[criterion1]
    print(f"方案1 - 起飞延误>15分钟: {len(delayed1)} 班 ({len(delayed1)/len(valid_data)*100:.1f}%)")
    
    # 方案2: 基于实际离港时间的延误
    criterion2 = valid_data['离港延误'] > 15  # 离港延误超过15分钟
    delayed2 = valid_data[criterion2]
    print(f"方案2 - 离港延误>15分钟: {len(delayed2)} 班 ({len(delayed2)/len(valid_data)*100:.1f}%)")
    
    # 方案3: 综合考虑 - 起飞延误或离港到起飞间隔过长
    criterion3 = (valid_data['起飞延误'] > 15) | (valid_data['离港到起飞间隔'] > 30)
    delayed3 = valid_data[criterion3]
    print(f"方案3 - 起飞延误>15分钟 OR 地面等待>30分钟: {len(delayed3)} 班 ({len(delayed3)/len(valid_data)*100:.1f}%)")
    
    # 方案4: 严格标准 - 起飞延误并且地面等待时间长
    criterion4 = (valid_data['起飞延误'] > 15) & (valid_data['离港到起飞间隔'] > 20)
    delayed4 = valid_data[criterion4]
    print(f"方案4 - 起飞延误>15分钟 AND 地面等待>20分钟: {len(delayed4)} 班 ({len(delayed4)/len(valid_data)*100:.1f}%)")
    
    return {
        'data': valid_data,
        'delayed1': delayed1,
        'delayed2': delayed2, 
        'delayed3': delayed3,
        'delayed4': delayed4
    }

def analyze_backlog_periods(delay_results):
    """分析积压时段 - 延误航班首次超过10班的时段"""
    if delay_results is None:
        return None
    
    print(f"\n=== 积压时段分析 ===")
    
    # 选择方案1作为主要分析对象（起飞延误>15分钟）
    delayed_flights = delay_results['delayed1'].copy()
    
    if len(delayed_flights) == 0:
        print("没有符合延误标准的航班")
        return None
    
    # 按小时分组统计延误航班数
    delayed_flights['hour'] = delayed_flights['计划离港时间'].dt.hour
    hourly_delays = delayed_flights.groupby('hour').size()
    
    print(f"\n=== 各时段延误航班统计 ===")
    for hour in range(24):
        delay_count = hourly_delays.get(hour, 0)
        backlog_status = "【积压】" if delay_count > 10 else ""
        print(f"{hour:02d}:00-{hour+1:02d}:00  延误航班: {delay_count:3d} 班 {backlog_status}")
    
    # 识别积压时段
    backlog_hours = hourly_delays[hourly_delays > 10].index.tolist()
    print(f"\n=== 识别出的积压时段 ===")
    if backlog_hours:
        print(f"积压时段: {backlog_hours}")
        for hour in backlog_hours:
            print(f"  {hour:02d}:00-{hour+1:02d}:00: {hourly_delays[hour]} 班延误航班")
    else:
        print("未发现延误航班超过10班的时段")
    
    # 可视化
    plt.figure(figsize=(15, 8))
    
    plt.subplot(2, 2, 1)
    hours = range(24)
    delay_counts = [hourly_delays.get(h, 0) for h in hours]
    bars = plt.bar(hours, delay_counts, alpha=0.7)
    
    # 标记积压时段
    for i, count in enumerate(delay_counts):
        if count > 10:
            bars[i].set_color('red')
    
    plt.axhline(y=10, color='red', linestyle='--', alpha=0.7, label='积压阈值(10班)')
    plt.xlabel('小时')
    plt.ylabel('延误航班数')
    plt.title('ZGGG各时段延误航班数量分布')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # 分析不同延误标准下的积压情况
    plt.subplot(2, 2, 2)
    standards = ['方案1\n起飞延误>15min', '方案2\n离港延误>15min', '方案3\n综合标准', '方案4\n严格标准']
    backlog_counts = []
    
    for i, key in enumerate(['delayed1', 'delayed2', 'delayed3', 'delayed4']):
        delayed_data = delay_results[key].copy()
        if len(delayed_data) > 0:
            delayed_data['hour'] = delayed_data['计划离港时间'].dt.hour
            hourly_count = delayed_data.groupby('hour').size()
            backlog_periods = len(hourly_count[hourly_count > 10])
        else:
            backlog_periods = 0
        backlog_counts.append(backlog_periods)
    
    plt.bar(standards, backlog_counts, alpha=0.7)
    plt.ylabel('积压时段数')
    plt.title('不同延误标准下的积压时段数量')
    plt.xticks(rotation=45)
    
    # 延误时长分布
    plt.subplot(2, 2, 3)
    delay_minutes = delayed_flights['起飞延误']
    plt.hist(delay_minutes, bins=30, alpha=0.7, edgecolor='black')
    plt.xlabel('起飞延误时间(分钟)')
    plt.ylabel('航班数量')
    plt.title('延误航班的延误时长分布')
    plt.grid(True, alpha=0.3)
    
    # 时间序列分析 - 按日期统计
    plt.subplot(2, 2, 4)
    delayed_flights['date'] = delayed_flights['计划离港时间'].dt.date
    daily_delays = delayed_flights.groupby('date').size()
    
    plt.plot(daily_delays.index, daily_delays.values, marker='o', alpha=0.7)
    plt.xlabel('日期')
    plt.ylabel('延误航班数')
    plt.title('每日延误航班数量趋势')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('ZGGG起飞延误积压分析.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    return {
        'hourly_delays': hourly_delays,
        'backlog_hours': backlog_hours,
        'delayed_flights': delayed_flights
    }

def main():
    """主函数"""
    print("=== ZGGG起飞延误积压分析 ===")
    print("专门分析ZGGG起飞航班延误情况和积压时段识别\n")
    
    # 1. 载入并分析数据结构
    df = load_and_analyze_data()
    if df is None:
        return
    
    # 2. 提取ZGGG起飞航班
    zggg_dep = extract_zggg_departures(df)
    
    # 3. 分析时间字段关系
    valid_data = analyze_time_relationships(zggg_dep)
    
    # 4. 提出延误判定标准
    delay_results = propose_delay_criteria(valid_data)
    
    # 5. 分析积压时段
    backlog_analysis = analyze_backlog_periods(delay_results)
    
    print(f"\n=== 分析完成 ===")
    print("建议的延误判定标准:")
    print("1. 主推荐: 起飞延误 > 15分钟 (相对宽松，捕捉更多延误)")
    print("2. 备选: 综合标准 - 起飞延误>15分钟 OR 地面等待>30分钟")
    print("3. 积压识别: 某时段延误航班数 > 10班")

if __name__ == "__main__":
    main()
