#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ZGGG机场原始数据分析
分析5月航班运行数据（脱敏）.xlsx中的ZGGG机场起降情况
重点关注延误和拥堵时段
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei']
plt.rcParams['axes.unicode_minus'] = False

def load_and_analyze_zggg_data():
    """加载并分析ZGGG机场数据"""
    
    print("=" * 60)
    print("ZGGG机场原始数据分析")
    print("=" * 60)
    
    # 1. 加载数据
    file_path = "数据/5月航班运行数据（脱敏）.xlsx"
    print(f"正在加载数据文件: {file_path}")
    
    try:
        # 尝试读取Excel文件
        df = pd.read_excel(file_path)
        print(f"成功加载数据: {len(df)} 条记录")
        print(f"数据列: {list(df.columns)}")
        
    except Exception as e:
        print(f"加载数据失败: {e}")
        return None
    
    # 2. 数据概览
    print("\n" + "=" * 40)
    print("数据概览")
    print("=" * 40)
    print("前5行数据:")
    print(df.head())
    
    print("\n数据基本信息:")
    print(df.info())
    
    # 3. 提取ZGGG相关航班
    print("\n" + "=" * 40)
    print("提取ZGGG相关航班")
    print("=" * 40)
    
    # 查找包含ZGGG的列
    zggg_columns = []
    for col in df.columns:
        if 'ZGGG' in str(df[col].unique()):
            zggg_columns.append(col)
    
    print(f"包含ZGGG的列: {zggg_columns}")
    
    # 尝试不同的列名组合来识别起降机场
    departure_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in ['起飞', '出发', 'dep', 'origin', '起始'])]
    arrival_cols = [col for col in df.columns if any(keyword in col.lower() for keyword in ['降落', '到达', 'arr', 'dest', '目的'])]
    
    print(f"可能的起飞机场列: {departure_cols}")
    print(f"可能的降落机场列: {arrival_cols}")
    
    # 提取ZGGG相关航班
    zggg_flights = pd.DataFrame()
    
    for dep_col in departure_cols:
        if dep_col in df.columns:
            departures = df[df[dep_col] == 'ZGGG'].copy()
            if not departures.empty:
                departures['flight_type'] = 'departure'
                departures['airport_col'] = dep_col
                zggg_flights = pd.concat([zggg_flights, departures], ignore_index=True)
    
    for arr_col in arrival_cols:
        if arr_col in df.columns:
            arrivals = df[df[arr_col] == 'ZGGG'].copy()
            if not arrivals.empty:
                arrivals['flight_type'] = 'arrival'
                arrivals['airport_col'] = arr_col
                zggg_flights = pd.concat([zggg_flights, arrivals], ignore_index=True)
    
    print(f"\n找到ZGGG相关航班: {len(zggg_flights)} 条")
    
    if len(zggg_flights) == 0:
        print("未找到ZGGG相关航班，检查所有机场代码...")
        
        # 检查所有唯一的机场代码
        for col in df.columns:
            if df[col].dtype == 'object':
                unique_values = df[col].unique()
                airport_codes = [v for v in unique_values if isinstance(v, str) and len(v) == 4 and v.isupper()]
                if airport_codes:
                    print(f"列 '{col}' 包含机场代码: {airport_codes[:10]}...")  # 只显示前10个
                    
                    if 'ZGGG' in airport_codes:
                        print(f"  -> 发现ZGGG在列 '{col}'")
        return df
    
    # 4. 时间分析
    print("\n" + "=" * 40)
    print("时间数据分析")
    print("=" * 40)
    
    # 查找时间相关列
    time_cols = [col for col in zggg_flights.columns if any(keyword in col.lower() for keyword in ['时间', 'time', '起飞', '降落', '到达'])]
    print(f"时间相关列: {time_cols}")
    
    # 分析每个时间列
    for col in time_cols:
        if col in zggg_flights.columns:
            print(f"\n列 '{col}' 的时间数据样本:")
            sample_data = zggg_flights[col].dropna().head(10)
            print(sample_data.values)
            
            # 尝试转换为datetime
            try:
                zggg_flights[col + '_parsed'] = pd.to_datetime(zggg_flights[col])
                print(f"  成功解析为datetime格式")
            except:
                print(f"  无法解析为datetime格式")
    
    # 5. 按起降类型统计
    print("\n" + "=" * 40)
    print("起降类型统计")
    print("=" * 40)
    
    flight_type_counts = zggg_flights['flight_type'].value_counts()
    print("起降类型分布:")
    for flight_type, count in flight_type_counts.items():
        print(f"  {flight_type}: {count} 架次")
    
    # 6. 寻找时间模式和拥堵
    print("\n" + "=" * 40)
    print("时间模式和拥堵分析")
    print("=" * 40)
    
    # 找到最主要的时间列进行分析
    main_time_col = None
    for col in time_cols:
        parsed_col = col + '_parsed'
        if parsed_col in zggg_flights.columns:
            non_null_count = zggg_flights[parsed_col].notna().sum()
            if non_null_count > 0:
                main_time_col = parsed_col
                print(f"使用主要时间列: {col} (有效数据: {non_null_count})")
                break
    
    if main_time_col:
        # 提取小时信息
        zggg_flights['hour'] = zggg_flights[main_time_col].dt.hour
        zggg_flights['date'] = zggg_flights[main_time_col].dt.date
        
        # 按小时统计航班数量
        hourly_counts = zggg_flights.groupby(['hour', 'flight_type']).size().unstack(fill_value=0)
        print("\n每小时航班数量统计:")
        print(hourly_counts)
        
        # 找出拥堵时段（航班数量最多的时段）
        total_hourly = zggg_flights.groupby('hour').size().sort_values(ascending=False)
        print(f"\n最繁忙的5个时段:")
        for hour, count in total_hourly.head().items():
            print(f"  {hour:02d}:00-{hour:02d}:59  {count} 架次")
        
        # 按日期分析
        daily_counts = zggg_flights.groupby(['date', 'flight_type']).size().unstack(fill_value=0)
        daily_total = zggg_flights.groupby('date').size().sort_values(ascending=False)
        
        print(f"\n最繁忙的5天:")
        for date, count in daily_total.head().items():
            print(f"  {date}  {count} 架次")
    
    # 7. 延误分析（如果有计划时间和实际时间）
    print("\n" + "=" * 40)
    print("延误分析")
    print("=" * 40)
    
    # 查找计划时间和实际时间列
    planned_cols = [col for col in zggg_flights.columns if any(keyword in col.lower() for keyword in ['计划', 'plan', 'schedule', '预计'])]
    actual_cols = [col for col in zggg_flights.columns if any(keyword in col.lower() for keyword in ['实际', 'actual', '真实'])]
    
    print(f"计划时间相关列: {planned_cols}")
    print(f"实际时间相关列: {actual_cols}")
    
    # 尝试计算延误
    delay_calculated = False
    for planned_col in planned_cols:
        for actual_col in actual_cols:
            if planned_col in zggg_flights.columns and actual_col in zggg_flights.columns:
                try:
                    planned_time = pd.to_datetime(zggg_flights[planned_col])
                    actual_time = pd.to_datetime(zggg_flights[actual_col])
                    delay_minutes = (actual_time - planned_time).dt.total_seconds() / 60
                    
                    # 过滤合理的延误时间（-60分钟到480分钟）
                    valid_delays = delay_minutes[(delay_minutes >= -60) & (delay_minutes <= 480)]
                    
                    if len(valid_delays) > 0:
                        print(f"\n使用 {planned_col} 和 {actual_col} 计算延误:")
                        print(f"  有效延误数据: {len(valid_delays)} 条")
                        print(f"  平均延误: {valid_delays.mean():.1f} 分钟")
                        print(f"  延误中位数: {valid_delays.median():.1f} 分钟")
                        print(f"  最大延误: {valid_delays.max():.1f} 分钟")
                        print(f"  最大提前: {valid_delays.min():.1f} 分钟")
                        
                        # 延误分布
                        on_time = len(valid_delays[abs(valid_delays) <= 15])
                        delayed = len(valid_delays[valid_delays > 15])
                        early = len(valid_delays[valid_delays < -15])
                        
                        print(f"  准点率 (±15分钟): {on_time/len(valid_delays)*100:.1f}%")
                        print(f"  延误率 (>15分钟): {delayed/len(valid_delays)*100:.1f}%")
                        print(f"  提前率 (<-15分钟): {early/len(valid_delays)*100:.1f}%")
                        
                        delay_calculated = True
                        break
                except Exception as e:
                    continue
        if delay_calculated:
            break
    
    if not delay_calculated:
        print("未找到可用于计算延误的时间列组合")
    
    return zggg_flights

def create_visualization(zggg_flights):
    """创建可视化图表"""
    
    if zggg_flights is None or len(zggg_flights) == 0:
        print("无数据可供可视化")
        return
    
    print("\n" + "=" * 40)
    print("生成可视化图表")
    print("=" * 40)
    
    # 创建图表
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('ZGGG机场航班运行分析', fontsize=16)
    
    # 1. 起降类型分布
    if 'flight_type' in zggg_flights.columns:
        flight_counts = zggg_flights['flight_type'].value_counts()
        axes[0, 0].pie(flight_counts.values, labels=flight_counts.index, autopct='%1.1f%%')
        axes[0, 0].set_title('起降类型分布')
    
    # 2. 每小时航班分布
    if 'hour' in zggg_flights.columns:
        hourly_data = zggg_flights.groupby('hour').size()
        axes[0, 1].bar(hourly_data.index, hourly_data.values)
        axes[0, 1].set_title('每小时航班数量')
        axes[0, 1].set_xlabel('小时')
        axes[0, 1].set_ylabel('航班数量')
    
    # 3. 每日航班分布
    if 'date' in zggg_flights.columns:
        daily_data = zggg_flights.groupby('date').size()
        axes[1, 0].plot(daily_data.index, daily_data.values, marker='o')
        axes[1, 0].set_title('每日航班数量趋势')
        axes[1, 0].set_xlabel('日期')
        axes[1, 0].set_ylabel('航班数量')
        axes[1, 0].tick_params(axis='x', rotation=45)
    
    # 4. 起降类型按小时分布
    if 'hour' in zggg_flights.columns and 'flight_type' in zggg_flights.columns:
        hourly_type = zggg_flights.groupby(['hour', 'flight_type']).size().unstack(fill_value=0)
        hourly_type.plot(kind='bar', stacked=True, ax=axes[1, 1])
        axes[1, 1].set_title('起降类型按小时分布')
        axes[1, 1].set_xlabel('小时')
        axes[1, 1].set_ylabel('航班数量')
        axes[1, 1].legend()
    
    plt.tight_layout()
    plt.savefig('ZGGG机场航班分析.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("图表已保存为 'ZGGG机场航班分析.png'")

if __name__ == "__main__":
    # 分析数据
    zggg_data = load_and_analyze_zggg_data()
    
    # 生成图表
    if zggg_data is not None and len(zggg_data) > 0:
        create_visualization(zggg_data)
    
    print("\n" + "=" * 60)
    print("分析完成")
    print("=" * 60)
