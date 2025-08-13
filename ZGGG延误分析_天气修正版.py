#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG起飞延误积压分析 - 天气停飞识别版
识别天气停飞时段，正确计算延误和积压
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

def load_and_clean_data():
    """载入数据并进行清洗"""
    print("=== 数据载入与初步清洗 ===")
    
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

def calculate_delays_and_identify_weather_events(data):
    """计算延误并识别天气停飞事件"""
    print(f"\n=== 延误计算与天气停飞事件识别 ===")
    
    # 计算基础延误时间
    data['原始起飞延误分钟'] = (data['实际起飞时间'] - data['计划离港时间']).dt.total_seconds() / 60
    data['地面滑行分钟'] = (data['实际起飞时间'] - data['实际离港时间']).dt.total_seconds() / 60
    
    print(f"延误计算公式: 实际起飞时间 - 计划离港时间")
    
    # 识别可能的天气停飞事件
    # 标准1: 延误超过4小时(240分钟)可能是天气原因
    potential_weather_delays = data[data['原始起飞延误分钟'] > 240].copy()
    print(f"\n发现可能的天气延误航班: {len(potential_weather_delays)} 班")
    
    if len(potential_weather_delays) > 0:
        # 按日期分组分析天气事件
        potential_weather_delays['date'] = potential_weather_delays['计划离港时间'].dt.date
        potential_weather_delays['actual_departure_hour'] = potential_weather_delays['实际离港时间'].dt.hour
        
        print(f"\n=== 疑似天气停飞事件分析 ===")
        weather_events = {}
        
        for date in potential_weather_delays['date'].unique():
            day_weather = potential_weather_delays[potential_weather_delays['date'] == date]
            if len(day_weather) >= 3:  # 一天有3班以上长延误，可能是天气
                # 分析实际起飞的集中时段
                actual_hours = day_weather['actual_departure_hour'].value_counts()
                concentrated_hours = actual_hours[actual_hours >= 2].index.tolist()
                
                if concentrated_hours:
                    weather_events[date] = {
                        'affected_flights': len(day_weather),
                        'concentrated_hours': concentrated_hours,
                        'flights': day_weather
                    }
                    
                    print(f"  {date}: {len(day_weather)}班长延误, 集中在 {concentrated_hours} 时段起飞")
        
        return data, weather_events
    
    return data, {}

def identify_weather_suspended_periods(data, weather_events):
    """识别天气停飞时段"""
    print(f"\n=== 天气停飞时段识别 ===")
    
    suspended_periods = []
    
    for date, event_info in weather_events.items():
        flights = event_info['flights']
        
        # 找出这一天最早的计划起飞和最晚的实际起飞
        min_planned = flights['计划离港时间'].min()
        max_actual = flights['实际起飞时间'].max()
        
        # 分析停飞时段：从最早计划时间到集中起飞时间之前
        concentrated_hours = event_info['concentrated_hours']
        if concentrated_hours:
            # 假设停飞结束时间是集中起飞时段的开始
            resume_hour = min(concentrated_hours)
            suspend_end = pd.Timestamp(date) + pd.Timedelta(hours=resume_hour)
            
            # 估算停飞开始时间（基于最早计划起飞前的合理时间）
            earliest_planned_hour = min_planned.hour
            suspend_start = pd.Timestamp(date) + pd.Timedelta(hours=max(0, earliest_planned_hour-1))
            
            suspended_periods.append({
                'date': date,
                'suspend_start': suspend_start,
                'suspend_end': suspend_end,
                'affected_flights': len(flights),
                'resume_hour': resume_hour
            })
            
            print(f"  识别停飞时段: {date} {suspend_start.strftime('%H:%M')}-{suspend_end.strftime('%H:%M')}")
            print(f"    影响航班: {len(flights)}班, 恢复时段: {resume_hour}点")
    
    return suspended_periods

def recalculate_delays_excluding_weather(data, suspended_periods):
    """重新计算延误，排除天气停飞影响"""
    print(f"\n=== 重新计算延误(排除天气影响) ===")
    
    # 复制数据
    adjusted_data = data.copy()
    adjusted_data['是否天气影响'] = False
    adjusted_data['调整后延误分钟'] = adjusted_data['原始起飞延误分钟']
    
    weather_affected_count = 0
    
    for period in suspended_periods:
        date = period['date']
        suspend_start = period['suspend_start']
        suspend_end = period['suspend_end']
        
        # 找到受影响的航班
        day_flights_mask = adjusted_data['计划离港时间'].dt.date == date
        
        # 情况1: 计划在停飞时段内的航班 - 从恢复时间开始计算延误
        planned_in_suspend = (
            day_flights_mask & 
            (adjusted_data['计划离港时间'] >= suspend_start) & 
            (adjusted_data['计划离港时间'] <= suspend_end)
        )
        
        # 情况2: 实际起飞在恢复时段的航班 - 可能是积压释放
        actual_in_resume = (
            day_flights_mask &
            (adjusted_data['实际起飞时间'] >= suspend_end) &
            (adjusted_data['实际起飞时间'] <= suspend_end + pd.Timedelta(hours=4))  # 恢复后4小时内
        )
        
        weather_affected = planned_in_suspend | actual_in_resume
        weather_affected_flights = adjusted_data[weather_affected]
        
        if len(weather_affected_flights) > 0:
            # 对于天气影响的航班，重新计算延误
            for idx in weather_affected_flights.index:
                flight = adjusted_data.loc[idx]
                planned_time = flight['计划离港时间']
                actual_time = flight['实际起飞时间']
                
                # 如果计划时间在停飞时段内，从恢复时间开始算延误
                if planned_time >= suspend_start and planned_time <= suspend_end:
                    adjusted_delay = (actual_time - suspend_end).total_seconds() / 60
                else:
                    # 如果是积压航班，计算相对于恢复时间的额外延误
                    normal_delay = (actual_time - planned_time).total_seconds() / 60
                    if normal_delay > 60:  # 超过1小时认为受天气影响
                        estimated_normal_takeoff = max(planned_time, suspend_end)
                        adjusted_delay = (actual_time - estimated_normal_takeoff).total_seconds() / 60
                    else:
                        adjusted_delay = normal_delay
                
                adjusted_data.loc[idx, '调整后延误分钟'] = max(0, adjusted_delay)  # 不允许负延误
                adjusted_data.loc[idx, '是否天气影响'] = True
                weather_affected_count += 1
    
    print(f"天气影响航班数: {weather_affected_count} 班")
    
    # 统计调整前后的差异
    original_delayed = (data['原始起飞延误分钟'] > 15).sum()
    adjusted_delayed = (adjusted_data['调整后延误分钟'] > 15).sum()
    
    print(f"调整前延误航班(>15min): {original_delayed} 班")
    print(f"调整后延误航班(>15min): {adjusted_delayed} 班")
    print(f"减少延误航班: {original_delayed - adjusted_delayed} 班")
    
    return adjusted_data

def analyze_corrected_patterns(adjusted_data):
    """分析修正后的延误模式"""
    print(f"\n=== 修正后延误模式分析 ===")
    
    # 基本数据清洗 - 移除极端异常值
    clean_data = adjusted_data[
        (adjusted_data['调整后延误分钟'] >= -30) &  # 提前不超过30分钟
        (adjusted_data['调整后延误分钟'] <= 240) &   # 延误不超过4小时
        (adjusted_data['地面滑行分钟'] >= 5) &       # 地面滑行至少5分钟
        (adjusted_data['地面滑行分钟'] <= 60)        # 地面滑行不超过60分钟
    ].copy()
    
    print(f"清洗后有效数据: {len(clean_data)} 班")
    
    # 添加时间字段
    clean_data['date'] = clean_data['计划离港时间'].dt.date
    clean_data['hour'] = clean_data['计划离港时间'].dt.hour
    
    # 统计每日情况
    total_days = clean_data['date'].nunique()
    print(f"总天数: {total_days} 天")
    
    # 按小时统计总航班和延误航班
    hourly_stats = {}
    for hour in range(24):
        hour_flights = clean_data[clean_data['hour'] == hour]
        total_count = len(hour_flights)
        delayed_count = len(hour_flights[hour_flights['调整后延误分钟'] > 15])
        
        hourly_stats[hour] = {
            'total': total_count,
            'delayed': delayed_count,
            'delay_rate': (delayed_count / total_count * 100) if total_count > 0 else 0,
            'avg_total': total_count / total_days,
            'avg_delayed': delayed_count / total_days
        }
    
    # 显示修正后的统计
    print(f"\n=== 修正后各时段统计 ===")
    print("时段          总航班  延误航班  延误率   日均总量  日均延误  积压状态")
    print("-" * 70)
    
    backlog_hours = []
    for hour in range(24):
        stats = hourly_stats[hour]
        is_backlog = stats['avg_delayed'] > 10
        backlog_status = "【积压】" if is_backlog else ""
        
        if is_backlog:
            backlog_hours.append(hour)
        
        print(f"{hour:02d}:00-{hour+1:02d}:00  {stats['total']:6d}  {stats['delayed']:6d}  {stats['delay_rate']:5.1f}%  "
              f"{stats['avg_total']:6.1f}  {stats['avg_delayed']:6.1f}  {backlog_status}")
    
    return clean_data, hourly_stats, backlog_hours

def identify_weather_backlog_periods(clean_data, suspended_periods):
    """识别天气恢复后的积压时段"""
    print(f"\n=== 天气恢复后积压时段识别 ===")
    
    weather_backlog_periods = []
    
    for period in suspended_periods:
        date = period['date']
        suspend_end = period['suspend_end']
        resume_hour = period['resume_hour']
        
        # 分析恢复后2-4小时内的情况
        resume_window_start = suspend_end
        resume_window_end = suspend_end + pd.Timedelta(hours=4)
        
        # 统计恢复窗口内每小时的起飞数量
        day_flights = clean_data[clean_data['计划离港时间'].dt.date == date]
        
        resume_flights = day_flights[
            (day_flights['实际起飞时间'] >= resume_window_start) &
            (day_flights['实际起飞时间'] <= resume_window_end)
        ]
        
        if len(resume_flights) > 0:
            # 按实际起飞小时统计
            resume_hourly = resume_flights.groupby(resume_flights['实际起飞时间'].dt.hour).size()
            
            backlog_hours = []
            for hour, count in resume_hourly.items():
                if count > 10:  # 某小时起飞超过10班认为是积压释放
                    backlog_hours.append(hour)
            
            if backlog_hours:
                weather_backlog_periods.append({
                    'date': date,
                    'resume_time': suspend_end,
                    'backlog_hours': backlog_hours,
                    'total_resume_flights': len(resume_flights)
                })
                
                print(f"  {date} 恢复后积压: {backlog_hours} 时段, 共 {len(resume_flights)} 班")
    
    return weather_backlog_periods

def visualize_corrected_analysis(clean_data, hourly_stats, suspended_periods):
    """可视化修正后的分析结果"""
    plt.figure(figsize=(20, 15))
    
    # 1. 修正后的各时段延误分布
    plt.subplot(3, 3, 1)
    hours = list(range(24))
    total_counts = [hourly_stats[h]['avg_total'] for h in hours]
    delayed_counts = [hourly_stats[h]['avg_delayed'] for h in hours]
    
    x = np.arange(len(hours))
    width = 0.35
    
    plt.bar(x - width/2, total_counts, width, label='日均总航班', alpha=0.7, color='lightblue')
    plt.bar(x + width/2, delayed_counts, width, label='日均延误航班', alpha=0.7, color='orange')
    
    plt.axhline(y=10, color='red', linestyle='--', alpha=0.7, label='积压阈值')
    plt.xlabel('小时')
    plt.ylabel('日均航班数')
    plt.title('修正后各时段航班分布')
    plt.xticks(x, [f'{h:02d}' for h in hours])
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # 2. 修正后的延误率分布
    plt.subplot(3, 3, 2)
    delay_rates = [hourly_stats[h]['delay_rate'] for h in hours]
    bars = plt.bar(hours, delay_rates, alpha=0.7, color='green')
    
    # 标记不合理的高延误率
    for i, rate in enumerate(delay_rates):
        if rate > 80:  # 延误率超过80%标记为红色
            bars[i].set_color('red')
    
    plt.xlabel('小时')
    plt.ylabel('延误率(%)')
    plt.title('修正后各时段延误率')
    plt.grid(True, alpha=0.3)
    
    # 3. 原始延误 vs 修正延误对比
    plt.subplot(3, 3, 3)
    original_delays = clean_data[clean_data['原始起飞延误分钟'] > 0]['原始起飞延误分钟']
    adjusted_delays = clean_data[clean_data['调整后延误分钟'] > 0]['调整后延误分钟']
    
    plt.hist([original_delays, adjusted_delays], bins=50, alpha=0.7, 
             label=['原始延误', '修正延误'], color=['red', 'blue'])
    plt.xlabel('延误时间(分钟)')
    plt.ylabel('航班数量')
    plt.title('延误分布对比')
    plt.legend()
    plt.xlim(0, 240)
    
    # 4. 天气影响航班分析
    plt.subplot(3, 3, 4)
    weather_affected = clean_data[clean_data['是否天气影响']]
    normal_flights = clean_data[~clean_data['是否天气影响']]
    
    if len(weather_affected) > 0:
        weather_hourly = weather_affected.groupby('hour').size()
        normal_hourly = normal_flights.groupby('hour').size()
        
        plt.bar(hours, [weather_hourly.get(h, 0) for h in hours], 
                alpha=0.7, label='天气影响', color='red')
        plt.bar(hours, [normal_hourly.get(h, 0) for h in hours], 
                alpha=0.7, label='正常航班', color='blue', bottom=[weather_hourly.get(h, 0) for h in hours])
    
    plt.xlabel('小时')
    plt.ylabel('航班数量')
    plt.title('天气影响航班分布')
    plt.legend()
    
    # 5. 凌晨时段详细分析
    plt.subplot(3, 3, 5)
    early_hours = list(range(0, 6))
    early_total = [hourly_stats[h]['avg_total'] for h in early_hours]
    early_delayed = [hourly_stats[h]['avg_delayed'] for h in early_hours]
    early_rates = [hourly_stats[h]['delay_rate'] for h in early_hours]
    
    fig_ax = plt.gca()
    ax2 = fig_ax.twinx()
    
    bars1 = fig_ax.bar([h-0.2 for h in early_hours], early_total, 0.4, label='总航班', alpha=0.7)
    bars2 = fig_ax.bar([h+0.2 for h in early_hours], early_delayed, 0.4, label='延误航班', alpha=0.7)
    line = ax2.plot(early_hours, early_rates, 'ro-', label='延误率')
    
    fig_ax.set_xlabel('小时')
    fig_ax.set_ylabel('日均航班数')
    ax2.set_ylabel('延误率(%)')
    fig_ax.set_title('凌晨时段分析(00-05)')
    fig_ax.legend(loc='upper left')
    ax2.legend(loc='upper right')
    
    # 6-9. 天气停飞事件可视化
    if suspended_periods:
        plt.subplot(3, 3, 6)
        dates = [p['date'] for p in suspended_periods]
        affected_counts = [p['affected_flights'] for p in suspended_periods]
        resume_hours = [p['resume_hour'] for p in suspended_periods]
        
        plt.scatter(dates, resume_hours, s=[c*10 for c in affected_counts], alpha=0.7)
        plt.xlabel('日期')
        plt.ylabel('恢复时段')
        plt.title('天气停飞事件')
        plt.xticks(rotation=45)
        
        for i, (date, hour, count) in enumerate(zip(dates, resume_hours, affected_counts)):
            plt.annotate(f'{count}班', (date, hour), xytext=(5, 5), 
                        textcoords='offset points', fontsize=8)
    
    plt.tight_layout()
    plt.savefig('ZGGG延误分析_天气修正版.png', dpi=300, bbox_inches='tight')
    plt.show()

def main():
    """主函数"""
    print("=== ZGGG起飞延误积压分析 - 天气停飞识别版 ===")
    
    # 1. 载入和清洗数据
    data = load_and_clean_data()
    
    # 2. 计算延误并识别天气事件
    data_with_delays, weather_events = calculate_delays_and_identify_weather_events(data)
    
    # 3. 识别天气停飞时段
    suspended_periods = identify_weather_suspended_periods(data_with_delays, weather_events)
    
    # 4. 重新计算延误，排除天气影响
    adjusted_data = recalculate_delays_excluding_weather(data_with_delays, suspended_periods)
    
    # 5. 分析修正后的延误模式
    clean_data, hourly_stats, backlog_hours = analyze_corrected_patterns(adjusted_data)
    
    # 6. 识别天气恢复后的积压时段
    weather_backlog_periods = identify_weather_backlog_periods(clean_data, suspended_periods)
    
    # 7. 可视化修正后的结果
    visualize_corrected_analysis(clean_data, hourly_stats, suspended_periods)
    
    # 8. 生成最终报告
    print(f"\n" + "="*80)
    print(f"                ZGGG延误分析最终报告(天气修正版)")
    print(f"="*80)
    
    print(f"\n【天气停飞事件】")
    print(f"  识别天气停飞事件: {len(suspended_periods)} 次")
    for period in suspended_periods:
        print(f"    {period['date']}: 影响 {period['affected_flights']} 班, 恢复于 {period['resume_hour']}:00")
    
    print(f"\n【修正后积压时段】")
    print(f"  日均积压时段: {backlog_hours}")
    
    return clean_data, hourly_stats, suspended_periods

if __name__ == "__main__":
    clean_data, hourly_stats, suspended_periods = main()
