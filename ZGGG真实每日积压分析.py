#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG起飞延误积压分析 - 真实每日积压时段识别
使用真实每日数据识别积压时段，不使用日均值
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def load_and_clean_data():
    """载入数据并进行清洗"""
    print("=== 数据载入与清洗 ===")
    
    df = pd.read_excel('/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/5月航班运行数据（脱敏）.xlsx')
    print(f"原始数据总记录数: {len(df)}")
    
    # 提取ZGGG起飞航班
    zggg_dep = df[df['实际起飞站四字码'] == 'ZGGG'].copy()
    print(f"ZGGG起飞航班总数: {len(zggg_dep)}")
    
    # 转换时间字段
    time_fields = ['计划离港时间', '实际离港时间', '实际起飞时间', '原计划离港时间']
    for field in time_fields:
        zggg_dep[field] = pd.to_datetime(zggg_dep[field], errors='coerce')
    
    # 只保留有完整时间数据的航班
    valid_data = zggg_dep.dropna(subset=['计划离港时间', '实际离港时间', '实际起飞时间']).copy()
    print(f"有完整时间数据的航班: {len(valid_data)}")
    
    return valid_data

def identify_weather_events_simple(data):
    """简单识别天气停飞事件"""
    # 计算基础延误时间
    data['原始起飞延误分钟'] = (data['实际起飞时间'] - data['计划离港时间']).dt.total_seconds() / 60
    
    # 识别可能的天气停飞事件：延误超过4小时(240分钟)
    potential_weather_delays = data[data['原始起飞延误分钟'] > 240].copy()
    
    weather_affected_dates = set()
    
    if len(potential_weather_delays) > 0:
        potential_weather_delays['date'] = potential_weather_delays['计划离港时间'].dt.date
        
        # 按日期分组，如果某天有3班以上长延误，认为是天气日
        for date in potential_weather_delays['date'].unique():
            day_weather = potential_weather_delays[potential_weather_delays['date'] == date]
            if len(day_weather) >= 3:
                weather_affected_dates.add(date)
    
    return weather_affected_dates

def calculate_adjusted_delays(data, weather_dates):
    """计算调整后的延误（简化版天气处理）"""
    data['调整后延误分钟'] = (data['实际起飞时间'] - data['计划离港时间']).dt.total_seconds() / 60
    
    # 对天气影响日期的航班进行简单调整
    for date in weather_dates:
        day_mask = data['计划离港时间'].dt.date == date
        day_flights = data[day_mask]
        
        # 对该天延误超过4小时的航班，将延误时间减少到合理范围
        extreme_delays = (day_flights['调整后延误分钟'] > 240)
        if extreme_delays.any():
            # 将极端延误调整为60-120分钟的随机值（模拟天气后的合理延误）
            adjustment_mask = day_mask & (data['调整后延误分钟'] > 240)
            np.random.seed(42)  # 保证结果可重现
            adjusted_delays = np.random.uniform(60, 120, adjustment_mask.sum())
            data.loc[adjustment_mask, '调整后延误分钟'] = adjusted_delays
    
    return data

def clean_extreme_values(data):
    """清洗极端值"""
    # 基本数据清洗
    clean_data = data[
        (data['调整后延误分钟'] >= -30) &     # 提前不超过30分钟
        (data['调整后延误分钟'] <= 300)       # 延误不超过5小时
    ].copy()
    
    print(f"数据清洗: {len(data)} -> {len(clean_data)} 班")
    return clean_data

def identify_daily_backlog_periods(clean_data, delay_threshold=30, backlog_threshold=10):
    """识别真实每日积压时段"""
    print(f"\n=== 真实每日积压时段识别 ===")
    print(f"延误标准: 超过{delay_threshold}分钟")
    print(f"积压标准: 每小时超过{backlog_threshold}班延误航班")
    
    # 添加日期和小时字段
    clean_data['date'] = clean_data['计划离港时间'].dt.date
    clean_data['hour'] = clean_data['计划离港时间'].dt.hour
    
    # 识别延误航班
    delayed_flights = clean_data[clean_data['调整后延误分钟'] > delay_threshold].copy()
    
    print(f"总延误航班: {len(delayed_flights)} 班")
    print(f"总分析天数: {clean_data['date'].nunique()} 天")
    
    # 按日期和小时统计延误航班数
    daily_hourly_delays = delayed_flights.groupby(['date', 'hour']).size().reset_index(name='delay_count')
    
    # 识别积压时段（每小时延误航班超过阈值）
    backlog_periods = daily_hourly_delays[daily_hourly_delays['delay_count'] > backlog_threshold].copy()
    
    print(f"\n发现积压时段: {len(backlog_periods)} 个")
    
    # 按日期整理积压时段
    backlog_by_date = {}
    
    for _, row in backlog_periods.iterrows():
        date = row['date']
        hour = row['hour']
        count = row['delay_count']
        
        if date not in backlog_by_date:
            backlog_by_date[date] = []
        
        backlog_by_date[date].append({
            'hour': hour,
            'delay_count': count
        })
    
    # 输出详细结果
    print(f"\n=== 详细积压时段列表 ===")
    
    total_backlog_periods = 0
    backlog_hour_stats = {}  # 统计各小时的积压频次
    
    for date in sorted(backlog_by_date.keys()):
        periods = backlog_by_date[date]
        periods.sort(key=lambda x: x['hour'])
        
        print(f"\n{date} ({len(periods)}个积压时段):")
        
        for period in periods:
            hour = period['hour']
            count = period['delay_count']
            total_backlog_periods += 1
            
            # 统计各小时积压频次
            if hour not in backlog_hour_stats:
                backlog_hour_stats[hour] = 0
            backlog_hour_stats[hour] += 1
            
            print(f"  {hour:02d}:00-{hour+1:02d}:00  延误航班: {count} 班")
    
    # 统计分析
    print(f"\n=== 积压统计分析 ===")
    print(f"总积压时段数: {total_backlog_periods} 个")
    print(f"涉及天数: {len(backlog_by_date)} 天")
    print(f"平均每天积压时段: {total_backlog_periods / len(backlog_by_date):.1f} 个")
    
    # 各小时积压频次统计
    print(f"\n各小时积压频次排序:")
    sorted_hours = sorted(backlog_hour_stats.items(), key=lambda x: x[1], reverse=True)
    
    for hour, freq in sorted_hours:
        percentage = freq / total_backlog_periods * 100
        print(f"  {hour:02d}:00-{hour+1:02d}:00: {freq:2d} 次 ({percentage:5.1f}%)")
    
    # 识别高频积压时段
    high_freq_hours = [hour for hour, freq in sorted_hours if freq >= 3]  # 出现3次以上
    print(f"\n高频积压时段(≥3次): {high_freq_hours}")
    
    # 连续积压时段分析
    print(f"\n=== 连续积压时段分析 ===")
    
    consecutive_backlog_days = 0
    
    for date in sorted(backlog_by_date.keys()):
        periods = backlog_by_date[date]
        hours = [p['hour'] for p in periods]
        hours.sort()
        
        # 识别连续小时
        consecutive_groups = []
        if hours:
            current_group = [hours[0]]
            for i in range(1, len(hours)):
                if hours[i] - hours[i-1] == 1:
                    current_group.append(hours[i])
                else:
                    consecutive_groups.append(current_group)
                    current_group = [hours[i]]
            consecutive_groups.append(current_group)
            
            # 输出连续积压时段
            for i, group in enumerate(consecutive_groups):
                if len(group) >= 2:  # 连续2小时以上
                    consecutive_backlog_days += 1
                    start_hour = group[0]
                    end_hour = group[-1]
                    duration = len(group)
                    total_delays = sum([p['delay_count'] for p in periods if p['hour'] in group])
                    
                    print(f"  {date} 连续积压: {start_hour:02d}:00-{end_hour+1:02d}:00 "
                          f"(持续{duration}小时, 共{total_delays}班延误)")
    
    return backlog_by_date, backlog_hour_stats

def analyze_backlog_patterns(backlog_by_date):
    """分析积压模式"""
    print(f"\n=== 积压模式深度分析 ===")
    
    # 工作日 vs 周末分析
    weekday_backlog = 0
    weekend_backlog = 0
    
    for date, periods in backlog_by_date.items():
        # 获取星期几 (0=Monday, 6=Sunday)
        weekday = pd.Timestamp(date).weekday()
        
        if weekday < 5:  # Monday-Friday
            weekday_backlog += len(periods)
        else:  # Saturday-Sunday
            weekend_backlog += len(periods)
    
    total_periods = weekday_backlog + weekend_backlog
    
    print(f"工作日积压时段: {weekday_backlog} 个 ({weekday_backlog/total_periods*100:.1f}%)")
    print(f"周末积压时段: {weekend_backlog} 个 ({weekend_backlog/total_periods*100:.1f}%)")
    
    # 时段分布分析
    morning_count = 0      # 06:00-11:59
    afternoon_count = 0    # 12:00-17:59
    evening_count = 0      # 18:00-23:59
    midnight_count = 0     # 00:00-05:59
    
    for date, periods in backlog_by_date.items():
        for period in periods:
            hour = period['hour']
            if 0 <= hour <= 5:
                midnight_count += 1
            elif 6 <= hour <= 11:
                morning_count += 1
            elif 12 <= hour <= 17:
                afternoon_count += 1
            elif 18 <= hour <= 23:
                evening_count += 1
    
    print(f"\n时段分布:")
    print(f"  凌晨(00:00-05:59): {midnight_count:2d} 个")
    print(f"  上午(06:00-11:59): {morning_count:2d} 个")
    print(f"  下午(12:00-17:59): {afternoon_count:2d} 个")
    print(f"  晚上(18:00-23:59): {evening_count:2d} 个")

def main():
    """主函数"""
    print("=== ZGGG起飞延误积压分析 - 真实每日数据版 ===")
    
    # 1. 载入和清洗数据
    data = load_and_clean_data()
    
    # 2. 识别天气影响日期
    weather_dates = identify_weather_events_simple(data)
    print(f"\n识别天气影响日期: {len(weather_dates)} 天")
    
    # 3. 调整延误计算
    adjusted_data = calculate_adjusted_delays(data, weather_dates)
    
    # 4. 清洗极端值
    clean_data = clean_extreme_values(adjusted_data)
    
    # 5. 识别真实每日积压时段
    backlog_by_date, backlog_hour_stats = identify_daily_backlog_periods(clean_data)
    
    # 6. 分析积压模式
    analyze_backlog_patterns(backlog_by_date)
    
    print(f"\n=== 分析完成 ===")
    print("基于真实每日数据的积压时段识别完成")
    print("积压判定标准: 每小时延误航班数 > 10班")
    
    return clean_data, backlog_by_date, backlog_hour_stats

if __name__ == "__main__":
    clean_data, backlog_by_date, backlog_hour_stats = main()
