#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG起飞延误积压分析 - 改进版
剔除异常数据，分析真实的延误和积压情况
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

def calculate_time_differences_and_clean(data):
    """计算时间差并清洗异常数据"""
    print(f"\n=== 时间差计算与数据清洗 ===")
    
    # 计算时间差（分钟）
    # 延误时长计算公式：实际起飞时间 - 计划离港时间
    data['起飞延误分钟'] = (data['实际起飞时间'] - data['计划离港时间']).dt.total_seconds() / 60
    
    # 离港延误计算公式：实际离港时间 - 计划离港时间  
    data['离港延误分钟'] = (data['实际离港时间'] - data['计划离港时间']).dt.total_seconds() / 60
    
    # 地面滑行时间计算公式：实际起飞时间 - 实际离港时间
    data['地面滑行分钟'] = (data['实际起飞时间'] - data['实际离港时间']).dt.total_seconds() / 60
    
    print(f"延误时长计算公式: 实际起飞时间 - 计划离港时间")
    print(f"离港延误计算公式: 实际离港时间 - 计划离港时间") 
    print(f"地面滑行计算公式: 实际起飞时间 - 实际离港时间")
    
    # 分析异常数据
    print(f"\n=== 异常数据分析 ===")
    
    # 1. 起飞延误异常（超过8小时=480分钟认为异常）
    extreme_delay = data[data['起飞延误分钟'] > 480]
    print(f"起飞延误超过8小时的异常航班: {len(extreme_delay)} 班")
    if len(extreme_delay) > 0:
        print("异常延误航班详情:")
        for _, row in extreme_delay.head(10).iterrows():
            print(f"  序列号: {row['唯一序列号']}")
            print(f"    计划离港: {row['计划离港时间']}")
            print(f"    实际离港: {row['实际离港时间']}")
            print(f"    实际起飞: {row['实际起飞时间']}")
            print(f"    起飞延误: {row['起飞延误分钟']:.1f}分钟")
            print()
    
    # 2. 地面滑行异常（超过60分钟认为异常）
    long_taxiing = data[data['地面滑行分钟'] > 60]
    print(f"地面滑行超过60分钟的异常航班: {len(long_taxiing)} 班")
    if len(long_taxiing) > 0:
        print("异常地面滑行航班详情:")
        for _, row in long_taxiing.head(10).iterrows():
            print(f"  序列号: {row['唯一序列号']}")
            print(f"    实际离港: {row['实际离港时间']}")
            print(f"    实际起飞: {row['实际起飞时间']}")
            print(f"    地面滑行: {row['地面滑行分钟']:.1f}分钟")
            print()
    
    # 3. 时间逻辑异常（实际起飞早于实际离港）
    time_logic_error = data[data['地面滑行分钟'] < 0]
    print(f"时间逻辑异常航班(起飞早于离港): {len(time_logic_error)} 班")
    
    # 清洗数据 - 剔除异常值
    print(f"\n=== 数据清洗 ===")
    original_count = len(data)
    
    # 剔除条件：
    # 1. 起飞延误超过8小时(480分钟)
    # 2. 地面滑行超过60分钟
    # 3. 地面滑行时间为负数
    # 4. 起飞延误小于-60分钟（提前超过1小时也不合理）
    
    clean_data = data[
        (data['起飞延误分钟'] <= 480) &  # 起飞延误不超过8小时
        (data['起飞延误分钟'] >= -60) &   # 提前不超过1小时
        (data['地面滑行分钟'] >= 5) &     # 地面滑行至少5分钟
        (data['地面滑行分钟'] <= 60)      # 地面滑行不超过60分钟
    ].copy()
    
    cleaned_count = len(clean_data)
    removed_count = original_count - cleaned_count
    
    print(f"原始数据: {original_count} 班")
    print(f"清洗后数据: {cleaned_count} 班") 
    print(f"剔除异常数据: {removed_count} 班 ({removed_count/original_count*100:.1f}%)")
    
    # 显示清洗后的数据统计
    print(f"\n=== 清洗后数据统计 ===")
    print(f"起飞延误: 最小 {clean_data['起飞延误分钟'].min():.1f}分钟, 最大 {clean_data['起飞延误分钟'].max():.1f}分钟")
    print(f"地面滑行: 最小 {clean_data['地面滑行分钟'].min():.1f}分钟, 最大 {clean_data['地面滑行分钟'].max():.1f}分钟")
    print(f"起飞延误均值: {clean_data['起飞延误分钟'].mean():.1f}分钟")
    print(f"地面滑行均值: {clean_data['地面滑行分钟'].mean():.1f}分钟")
    
    return clean_data

def analyze_daily_patterns(clean_data):
    """分析每日模式，计算平均情况"""
    print(f"\n=== 日均延误模式分析 ===")
    
    # 添加日期和小时字段
    clean_data['date'] = clean_data['计划离港时间'].dt.date
    clean_data['hour'] = clean_data['计划离港时间'].dt.hour
    
    # 统计每天的数据
    total_days = clean_data['date'].nunique()
    print(f"分析时段: {clean_data['date'].min()} 至 {clean_data['date'].max()}")
    print(f"总天数: {total_days} 天")
    
    # 按日期和小时统计总航班数
    daily_hourly_total = clean_data.groupby(['date', 'hour']).size().reset_index(name='total_flights')
    avg_hourly_total = daily_hourly_total.groupby('hour')['total_flights'].mean()
    
    # 定义延误标准 - 起飞延误超过15分钟
    delayed_flights = clean_data[clean_data['起飞延误分钟'] > 15].copy()
    delayed_flights['date'] = delayed_flights['计划离港时间'].dt.date
    delayed_flights['hour'] = delayed_flights['计划离港时间'].dt.hour
    
    # 按日期和小时统计延误航班数
    daily_hourly_delays = delayed_flights.groupby(['date', 'hour']).size().reset_index(name='delayed_flights')
    avg_hourly_delays = daily_hourly_delays.groupby('hour')['delayed_flights'].mean()
    
    print(f"\n=== 各时段日均航班统计 ===")
    print("时段          总航班  延误航班  延误率   积压状态")
    print("-" * 50)
    
    backlog_hours = []
    for hour in range(24):
        total_avg = avg_hourly_total.get(hour, 0)
        delay_avg = avg_hourly_delays.get(hour, 0)
        delay_rate = (delay_avg / total_avg * 100) if total_avg > 0 else 0
        
        # 积压标准：日均延误航班超过10班
        is_backlog = delay_avg > 10
        backlog_status = "【积压】" if is_backlog else ""
        
        if is_backlog:
            backlog_hours.append(hour)
        
        print(f"{hour:02d}:00-{hour+1:02d}:00  {total_avg:6.1f}  {delay_avg:6.1f}  {delay_rate:5.1f}%  {backlog_status}")
    
    print(f"\n=== 积压时段识别 ===")
    print(f"积压时段: {backlog_hours}")
    
    # 识别连续积压时段
    if backlog_hours:
        continuous_periods = identify_continuous_periods(backlog_hours)
        print(f"\n连续积压时段:")
        for i, period in enumerate(continuous_periods, 1):
            start_hour = period[0]
            end_hour = period[-1]
            duration = len(period)
            total_avg_delays = sum([avg_hourly_delays.get(h, 0) for h in period])
            print(f"  积压{i}: {start_hour:02d}:00-{end_hour+1:02d}:00 (持续{duration}小时, 日均{total_avg_delays:.1f}班延误)")
    
    return {
        'avg_hourly_total': avg_hourly_total,
        'avg_hourly_delays': avg_hourly_delays, 
        'backlog_hours': backlog_hours,
        'total_days': total_days,
        'delayed_flights': delayed_flights
    }

def identify_continuous_periods(hours):
    """识别连续时段"""
    if not hours:
        return []
    
    continuous_periods = []
    current_period = [hours[0]]
    
    for i in range(1, len(hours)):
        if hours[i] - hours[i-1] == 1:
            current_period.append(hours[i])
        else:
            continuous_periods.append(current_period)
            current_period = [hours[i]]
    
    continuous_periods.append(current_period)
    return continuous_periods

def analyze_reasonable_backlog(daily_results):
    """分析合理的积压模式"""
    print(f"\n=== 合理积压模式分析 ===")
    
    backlog_hours = daily_results['backlog_hours']
    avg_hourly_delays = daily_results['avg_hourly_delays']
    
    # 过滤掉凌晨时段的积压（00:00-05:59认为不合理）
    reasonable_backlog = [h for h in backlog_hours if h >= 6]
    
    print(f"原积压时段: {backlog_hours}")
    print(f"合理积压时段(排除凌晨): {reasonable_backlog}")
    
    if reasonable_backlog:
        continuous_periods = identify_continuous_periods(reasonable_backlog)
        
        print(f"\n合理的连续积压时段:")
        valid_periods = []
        for i, period in enumerate(continuous_periods, 1):
            start_hour = period[0] 
            end_hour = period[-1]
            duration = len(period)
            avg_delays = sum([avg_hourly_delays.get(h, 0) for h in period]) / len(period)
            
            # 只保留持续2小时以上的积压时段
            if duration >= 2:
                valid_periods.append(period)
                print(f"  有效积压{len(valid_periods)}: {start_hour:02d}:00-{end_hour+1:02d}:00")
                print(f"    持续时间: {duration}小时")
                print(f"    平均延误: {avg_delays:.1f}班/小时")
                
                # 分析积压强度
                max_delay = max([avg_hourly_delays.get(h, 0) for h in period])
                max_hour = period[np.argmax([avg_hourly_delays.get(h, 0) for h in period])]
                print(f"    峰值时段: {max_hour:02d}:00-{max_hour+1:02d}:00 ({max_delay:.1f}班)")
        
        print(f"\n=== 积压模式总结 ===")
        print(f"发现 {len(valid_periods)} 个合理的积压时段")
        
        if len(valid_periods) <= 2:
            print("✓ 符合预期：一天有1-2个积压高峰")
        else:
            print("⚠ 可能需要进一步优化参数")
            
        return valid_periods
    else:
        print("未发现合理的积压时段")
        return []

def visualize_improved_analysis(clean_data, daily_results, valid_periods):
    """可视化改进后的分析结果"""
    plt.figure(figsize=(20, 12))
    
    # 1. 日均各时段延误航班分布
    plt.subplot(2, 3, 1)
    avg_hourly_delays = daily_results['avg_hourly_delays']
    hours = range(24)
    delay_counts = [avg_hourly_delays.get(h, 0) for h in hours]
    
    bars = plt.bar(hours, delay_counts, alpha=0.7, color='lightblue')
    
    # 标记积压时段
    for hour in daily_results['backlog_hours']:
        if hour < len(bars):
            bars[hour].set_color('red')
    
    # 标记合理积压时段
    for period in valid_periods:
        for hour in period:
            if hour < len(bars):
                bars[hour].set_color('darkred')
    
    plt.axhline(y=10, color='red', linestyle='--', alpha=0.7, label='积压阈值(10班/天)')
    plt.xlabel('小时')
    plt.ylabel('日均延误航班数')
    plt.title('各时段日均延误航班分布')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # 2. 清洗前后延误分布对比
    plt.subplot(2, 3, 2)
    # 这里需要原始数据进行对比，先显示清洗后的
    clean_delays = clean_data[clean_data['起飞延误分钟'] > 0]['起飞延误分钟']
    plt.hist(clean_delays, bins=50, alpha=0.7, color='green', label='清洗后')
    plt.axvline(x=15, color='red', linestyle='--', label='15分钟阈值')
    plt.xlabel('起飞延误时间(分钟)')
    plt.ylabel('航班数量')
    plt.title('清洗后延误时长分布')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # 3. 地面滑行时间分布
    plt.subplot(2, 3, 3)
    taxiing_time = clean_data['地面滑行分钟']
    plt.hist(taxiing_time, bins=40, alpha=0.7, color='orange')
    plt.axvline(x=taxiing_time.mean(), color='red', linestyle='--', 
                label=f'平均值: {taxiing_time.mean():.1f}分钟')
    plt.xlabel('地面滑行时间(分钟)')
    plt.ylabel('航班数量')
    plt.title('地面滑行时间分布(清洗后)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # 4. 各时段总航班vs延误航班
    plt.subplot(2, 3, 4)
    avg_hourly_total = daily_results['avg_hourly_total']
    total_counts = [avg_hourly_total.get(h, 0) for h in hours]
    
    x = np.arange(len(hours))
    width = 0.35
    
    plt.bar(x - width/2, total_counts, width, label='总航班', alpha=0.7)
    plt.bar(x + width/2, delay_counts, width, label='延误航班', alpha=0.7)
    
    plt.xlabel('小时')
    plt.ylabel('日均航班数')
    plt.title('各时段总航班vs延误航班')
    plt.xticks(x, [f'{h:02d}' for h in hours])
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # 5. 延误率分布
    plt.subplot(2, 3, 5)
    delay_rates = []
    for h in hours:
        total = avg_hourly_total.get(h, 0)
        delayed = avg_hourly_delays.get(h, 0)
        rate = (delayed / total * 100) if total > 0 else 0
        delay_rates.append(rate)
    
    plt.bar(hours, delay_rates, alpha=0.7, color='purple')
    plt.xlabel('小时')
    plt.ylabel('延误率(%)')
    plt.title('各时段延误率')
    plt.grid(True, alpha=0.3)
    
    # 6. 积压时段热力图
    plt.subplot(2, 3, 6)
    delayed_flights = daily_results['delayed_flights']
    if len(delayed_flights) > 0:
        # 创建日期-小时矩阵
        pivot_data = delayed_flights.groupby([delayed_flights['date'], 'hour']).size().unstack(fill_value=0)
        
        if len(pivot_data) > 0:
            plt.imshow(pivot_data.values, cmap='Reds', aspect='auto')
            plt.colorbar(label='延误航班数')
            plt.xlabel('小时')
            plt.ylabel('日期')
            plt.title('每日各时段延误航班热力图')
            
            # 设置x轴标签
            plt.xticks(range(0, 24, 4), [f'{h:02d}' for h in range(0, 24, 4)])
    
    plt.tight_layout()
    plt.savefig('ZGGG起飞延误改进分析.png', dpi=300, bbox_inches='tight')
    plt.show()

def generate_final_report(clean_data, daily_results, valid_periods):
    """生成最终报告"""
    print(f"\n" + "="*80)
    print(f"                    ZGGG起飞延误积压分析最终报告")
    print(f"="*80)
    
    print(f"\n【数据清洗情况】")
    total_days = daily_results['total_days']
    print(f"  分析时段: {total_days}天")
    print(f"  有效航班: {len(clean_data)}班")
    print(f"  日均航班: {len(clean_data)/total_days:.1f}班/天")
    
    print(f"\n【延误情况统计】")
    delayed_count = len(clean_data[clean_data['起飞延误分钟'] > 15])
    delay_rate = delayed_count / len(clean_data) * 100
    avg_delay = clean_data[clean_data['起飞延误分钟'] > 0]['起飞延误分钟'].mean()
    
    print(f"  延误航班数: {delayed_count}班")
    print(f"  延误率: {delay_rate:.1f}%")
    print(f"  平均延误: {avg_delay:.1f}分钟")
    print(f"  日均延误: {delayed_count/total_days:.1f}班/天")
    
    print(f"\n【积压时段分析】")
    print(f"  发现合理积压时段: {len(valid_periods)}个")
    
    for i, period in enumerate(valid_periods, 1):
        start_hour = period[0]
        end_hour = period[-1] 
        duration = len(period)
        avg_delays = sum([daily_results['avg_hourly_delays'].get(h, 0) for h in period])
        
        print(f"\n  积压时段{i}: {start_hour:02d}:00-{end_hour+1:02d}:00")
        print(f"    持续时间: {duration}小时")
        print(f"    日均延误: {avg_delays:.1f}班")
        print(f"    平均强度: {avg_delays/duration:.1f}班/小时")
    
    print(f"\n【仿真参数建议】")
    print(f"  延误判定标准: 起飞延误 > 15分钟")
    print(f"  积压触发阈值: 日均延误航班 > 10班")
    print(f"  数据清洗规则:")
    print(f"    - 起飞延误: [-60, 480]分钟")
    print(f"    - 地面滑行: [5, 60]分钟")
    print(f"  预期积压模式: {len(valid_periods)}个高峰时段")

def main():
    """主函数"""
    print("=== ZGGG起飞延误积压分析 - 改进版 ===")
    
    # 1. 载入和清洗数据
    data = load_and_clean_data()
    
    # 2. 计算时间差并清洗异常数据
    clean_data = calculate_time_differences_and_clean(data)
    
    # 3. 分析日均模式
    daily_results = analyze_daily_patterns(clean_data)
    
    # 4. 分析合理的积压模式
    valid_periods = analyze_reasonable_backlog(daily_results)
    
    # 5. 可视化
    visualize_improved_analysis(clean_data, daily_results, valid_periods)
    
    # 6. 生成最终报告
    generate_final_report(clean_data, daily_results, valid_periods)
    
    return clean_data, daily_results, valid_periods

if __name__ == "__main__":
    clean_data, daily_results, valid_periods = main()
