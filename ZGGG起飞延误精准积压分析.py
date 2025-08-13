#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG起飞延误精准积压分析
根据数据特征优化延误判定标准和积压识别逻辑
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

def load_and_process_data():
    """载入数据并预处理"""
    df = pd.read_excel('/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/5月航班运行数据（脱敏）.xlsx')
    
    # 提取ZGGG起飞航班
    zggg_dep = df[df['实际起飞站四字码'] == 'ZGGG'].copy()
    
    # 转换时间字段
    time_fields = ['计划离港时间', '实际离港时间', '实际起飞时间', '原计划离港时间']
    for field in time_fields:
        zggg_dep[field] = pd.to_datetime(zggg_dep[field], errors='coerce')
    
    # 只保留有完整时间数据的航班
    valid_data = zggg_dep.dropna(subset=['计划离港时间', '实际离港时间', '实际起飞时间']).copy()
    
    # 计算关键时间差
    valid_data['离港延误分钟'] = (valid_data['实际离港时间'] - valid_data['计划离港时间']).dt.total_seconds() / 60
    valid_data['起飞延误分钟'] = (valid_data['实际起飞时间'] - valid_data['计划离港时间']).dt.total_seconds() / 60
    valid_data['地面滑行时间'] = (valid_data['实际起飞时间'] - valid_data['实际离港时间']).dt.total_seconds() / 60
    
    # 判断是否有调时
    valid_data['是否调时'] = valid_data['调时航班标识'] == 'Y'
    
    # 判断是否取消
    valid_data['是否取消'] = valid_data['取消时间'] != '-'
    
    print(f"载入ZGGG起飞航班: {len(valid_data)} 班")
    print(f"调时航班: {valid_data['是否调时'].sum()} 班 ({valid_data['是否调时'].sum()/len(valid_data)*100:.1f}%)")
    print(f"取消航班: {valid_data['是否取消'].sum()} 班 ({valid_data['是否取消'].sum()/len(valid_data)*100:.1f}%)")
    
    return valid_data

def analyze_delay_characteristics(data):
    """深入分析延误特征"""
    print(f"\n=== 延误特征深度分析 ===")
    
    # 统计各种延误情况
    delay_stats = {
        '起飞延误>0分钟': (data['起飞延误分钟'] > 0).sum(),
        '起飞延误>5分钟': (data['起飞延误分钟'] > 5).sum(),
        '起飞延误>10分钟': (data['起飞延误分钟'] > 10).sum(),
        '起飞延误>15分钟': (data['起飞延误分钟'] > 15).sum(),
        '起飞延误>30分钟': (data['起飞延误分钟'] > 30).sum(),
        '起飞延误>60分钟': (data['起飞延误分钟'] > 60).sum(),
    }
    
    print("延误航班统计:")
    for desc, count in delay_stats.items():
        pct = count / len(data) * 100
        print(f"  {desc}: {count:4d} 班 ({pct:5.1f}%)")
    
    # 分析地面滑行时间
    print(f"\n地面滑行时间统计:")
    taxiing_stats = data['地面滑行时间'].describe()
    print(f"  平均: {taxiing_stats['mean']:.1f} 分钟")
    print(f"  中位数: {taxiing_stats['50%']:.1f} 分钟")
    print(f"  75分位: {taxiing_stats['75%']:.1f} 分钟")
    print(f"  90分位: {data['地面滑行时间'].quantile(0.9):.1f} 分钟")
    print(f"  95分位: {data['地面滑行时间'].quantile(0.95):.1f} 分钟")
    
    # 分析调时航班的延误情况
    if data['是否调时'].sum() > 0:
        rescheduled = data[data['是否调时']]
        normal = data[~data['是否调时']]
        
        print(f"\n调时 vs 正常航班延误对比:")
        print(f"  调时航班起飞延误: 平均 {rescheduled['起飞延误分钟'].mean():.1f} 分钟")
        print(f"  正常航班起飞延误: 平均 {normal['起飞延误分钟'].mean():.1f} 分钟")
    
    return delay_stats

def define_delay_criteria(data):
    """定义多种延误判定标准"""
    print(f"\n=== 延误判定标准定义 ===")
    
    # 标准1: 基础延误标准 (起飞延误>15分钟)
    std1_mask = data['起飞延误分钟'] > 15
    std1_flights = data[std1_mask].copy()
    
    # 标准2: 严格延误标准 (起飞延误>15分钟 且 地面滑行>正常时间)
    normal_taxiing_threshold = data['地面滑行时间'].quantile(0.75)  # 75分位作为正常阈值
    std2_mask = (data['起飞延误分钟'] > 15) & (data['地面滑行时间'] > normal_taxiing_threshold)
    std2_flights = data[std2_mask].copy()
    
    # 标准3: 综合延误标准 (起飞延误>10分钟 或 地面滑行>30分钟)
    std3_mask = (data['起飞延误分钟'] > 10) | (data['地面滑行时间'] > 30)
    std3_flights = data[std3_mask].copy()
    
    # 标准4: 实用延误标准 (考虑调时情况)
    std4_mask = ((data['起飞延误分钟'] > 15) & ~data['是否调时']) | ((data['起飞延误分钟'] > 5) & data['是否调时'])
    std4_flights = data[std4_mask].copy()
    
    delay_criteria = {
        '标准1_基础延误': {'mask': std1_mask, 'flights': std1_flights, 'desc': '起飞延误>15分钟'},
        '标准2_严格延误': {'mask': std2_mask, 'flights': std2_flights, 'desc': f'起飞延误>15分钟 且 地面滑行>{normal_taxiing_threshold:.0f}分钟'},
        '标准3_综合延误': {'mask': std3_mask, 'flights': std3_flights, 'desc': '起飞延误>10分钟 或 地面滑行>30分钟'},
        '标准4_实用延误': {'mask': std4_mask, 'flights': std4_flights, 'desc': '区分调时(>5min)与正常(>15min)'},
    }
    
    print("延误标准统计:")
    for key, info in delay_criteria.items():
        count = len(info['flights'])
        pct = count / len(data) * 100
        print(f"  {key}: {count:4d} 班 ({pct:5.1f}%) - {info['desc']}")
    
    return delay_criteria, normal_taxiing_threshold

def identify_backlog_periods(delay_criteria, threshold=10):
    """识别积压时段"""
    print(f"\n=== 积压时段识别 (阈值: {threshold}班) ===")
    
    results = {}
    
    for std_name, info in delay_criteria.items():
        delayed_flights = info['flights'].copy()
        
        if len(delayed_flights) == 0:
            results[std_name] = {'backlog_hours': [], 'hourly_stats': None}
            continue
        
        # 按小时统计延误航班数
        delayed_flights['hour'] = delayed_flights['计划离港时间'].dt.hour
        hourly_stats = delayed_flights.groupby('hour').size()
        
        # 识别积压时段
        backlog_hours = hourly_stats[hourly_stats > threshold].index.tolist()
        
        results[std_name] = {
            'backlog_hours': backlog_hours,
            'hourly_stats': hourly_stats,
            'delayed_flights': delayed_flights
        }
        
        print(f"\n{std_name} - {info['desc']}:")
        print(f"  积压时段数: {len(backlog_hours)} 个")
        if backlog_hours:
            print(f"  积压时段: {sorted(backlog_hours)}")
            max_delay_hour = hourly_stats.idxmax()
            max_delay_count = hourly_stats.max()
            print(f"  最严重积压: {max_delay_hour:02d}:00-{max_delay_hour+1:02d}:00 ({max_delay_count}班)")
    
    return results

def analyze_backlog_patterns(backlog_results):
    """分析积压模式"""
    print(f"\n=== 积压模式分析 ===")
    
    # 选择基础延误标准进行详细分析
    std1_result = backlog_results['标准1_基础延误']
    
    if std1_result['hourly_stats'] is None:
        return None
    
    hourly_stats = std1_result['hourly_stats']
    delayed_flights = std1_result['delayed_flights']
    
    # 按时段分类
    time_periods = {
        '凌晨 (00:00-05:59)': list(range(0, 6)),
        '早晨 (06:00-11:59)': list(range(6, 12)),
        '下午 (12:00-17:59)': list(range(12, 18)),
        '晚上 (18:00-23:59)': list(range(18, 24))
    }
    
    print("各时段积压情况:")
    for period_name, hours in time_periods.items():
        period_delays = sum([hourly_stats.get(h, 0) for h in hours])
        backlog_hours_in_period = [h for h in hours if hourly_stats.get(h, 0) > 10]
        print(f"  {period_name}: 总延误 {period_delays:3d} 班, 积压时段 {len(backlog_hours_in_period)} 个")
    
    # 分析连续积压时段
    backlog_hours = sorted(std1_result['backlog_hours'])
    continuous_periods = []
    if backlog_hours:
        current_period = [backlog_hours[0]]
        for i in range(1, len(backlog_hours)):
            if backlog_hours[i] - backlog_hours[i-1] == 1:
                current_period.append(backlog_hours[i])
            else:
                continuous_periods.append(current_period)
                current_period = [backlog_hours[i]]
        continuous_periods.append(current_period)
        
        print(f"\n连续积压时段识别:")
        for i, period in enumerate(continuous_periods, 1):
            start_hour = period[0]
            end_hour = period[-1]
            duration = len(period)
            total_delays = sum([hourly_stats.get(h, 0) for h in period])
            print(f"  连续积压{i}: {start_hour:02d}:00-{end_hour+1:02d}:00 (持续{duration}小时, 共{total_delays}班延误)")
    
    return {
        'hourly_stats': hourly_stats,
        'delayed_flights': delayed_flights,
        'continuous_periods': continuous_periods,
        'backlog_hours': backlog_hours
    }

def visualize_results(data, backlog_results, backlog_patterns):
    """可视化分析结果"""
    plt.figure(figsize=(20, 15))
    
    # 1. 各时段延误航班分布 (基础标准)
    plt.subplot(3, 3, 1)
    if backlog_patterns and 'hourly_stats' in backlog_patterns:
        hourly_stats = backlog_patterns['hourly_stats']
        hours = range(24)
        counts = [hourly_stats.get(h, 0) for h in hours]
        bars = plt.bar(hours, counts, alpha=0.7, color='lightblue')
        
        # 标记积压时段
        for i, count in enumerate(counts):
            if count > 10:
                bars[i].set_color('red')
        
        plt.axhline(y=10, color='red', linestyle='--', alpha=0.7, label='积压阈值(10班)')
        plt.xlabel('小时')
        plt.ylabel('延误航班数')
        plt.title('各时段延误航班分布 (基础标准)')
        plt.legend()
        plt.grid(True, alpha=0.3)
    
    # 2. 不同延误标准对比
    plt.subplot(3, 3, 2)
    standards = ['标准1_基础延误', '标准2_严格延误', '标准3_综合延误', '标准4_实用延误']
    backlog_counts = []
    for std in standards:
        backlog_counts.append(len(backlog_results[std]['backlog_hours']))
    
    plt.bar([s.split('_')[1] for s in standards], backlog_counts, alpha=0.7)
    plt.ylabel('积压时段数')
    plt.title('不同延误标准的积压时段数量')
    plt.xticks(rotation=45)
    
    # 3. 延误时长分布
    plt.subplot(3, 3, 3)
    delay_minutes = data['起飞延误分钟']
    plt.hist(delay_minutes[delay_minutes > 0], bins=50, alpha=0.7, edgecolor='black')
    plt.axvline(x=15, color='red', linestyle='--', label='15分钟阈值')
    plt.xlabel('起飞延误时间(分钟)')
    plt.ylabel('航班数量')
    plt.title('延误航班的延误时长分布')
    plt.legend()
    
    # 4. 地面滑行时间分布
    plt.subplot(3, 3, 4)
    taxiing_time = data['地面滑行时间']
    plt.hist(taxiing_time, bins=50, alpha=0.7, edgecolor='black')
    plt.axvline(x=taxiing_time.quantile(0.75), color='orange', linestyle='--', label='75分位')
    plt.axvline(x=30, color='red', linestyle='--', label='30分钟')
    plt.xlabel('地面滑行时间(分钟)')
    plt.ylabel('航班数量')
    plt.title('地面滑行时间分布')
    plt.legend()
    
    # 5. 调时vs正常航班延误对比
    plt.subplot(3, 3, 5)
    rescheduled = data[data['是否调时']]['起飞延误分钟']
    normal = data[~data['是否调时']]['起飞延误分钟']
    
    plt.hist([normal, rescheduled], bins=30, alpha=0.7, 
             label=['正常航班', '调时航班'], color=['blue', 'red'])
    plt.xlabel('起飞延误时间(分钟)')
    plt.ylabel('航班数量')
    plt.title('调时vs正常航班延误对比')
    plt.legend()
    
    # 6-9. 各标准的时段分布
    for i, (std_name, result) in enumerate(list(backlog_results.items())[:4], 6):
        plt.subplot(3, 3, i)
        if result['hourly_stats'] is not None:
            hourly_stats = result['hourly_stats']
            hours = range(24)
            counts = [hourly_stats.get(h, 0) for h in hours]
            bars = plt.bar(hours, counts, alpha=0.7, color='lightgreen')
            
            for j, count in enumerate(counts):
                if count > 10:
                    bars[j].set_color('red')
                    
            plt.axhline(y=10, color='red', linestyle='--', alpha=0.7)
            plt.xlabel('小时')
            plt.ylabel('延误航班数')
            plt.title(f'{std_name.split("_")[1]}')
            if max(counts) > 0:
                plt.ylim(0, max(counts) * 1.1)
    
    plt.tight_layout()
    plt.savefig('ZGGG起飞延误精准积压分析.png', dpi=300, bbox_inches='tight')
    plt.show()

def generate_summary_report(data, delay_criteria, backlog_results, backlog_patterns):
    """生成总结报告"""
    print(f"\n" + "="*60)
    print(f"          ZGGG起飞延误积压分析总结报告")
    print(f"="*60)
    
    print(f"\n【数据概况】")
    print(f"  分析时段: 2025年5月")
    print(f"  ZGGG起飞航班总数: {len(data)} 班")
    print(f"  调时航班: {data['是否调时'].sum()} 班")
    print(f"  平均起飞延误: {data['起飞延误分钟'].mean():.1f} 分钟")
    print(f"  平均地面滑行: {data['地面滑行时间'].mean():.1f} 分钟")
    
    print(f"\n【延误标准推荐】")
    best_std = '标准1_基础延误'  # 根据实际情况选择最佳标准
    print(f"  推荐使用: {delay_criteria[best_std]['desc']}")
    print(f"  延误航班数: {len(delay_criteria[best_std]['flights'])} 班")
    print(f"  延误率: {len(delay_criteria[best_std]['flights'])/len(data)*100:.1f}%")
    
    print(f"\n【积压时段识别】")
    best_result = backlog_results[best_std]
    print(f"  积压时段总数: {len(best_result['backlog_hours'])} 个")
    if best_result['backlog_hours']:
        print(f"  积压时段列表: {sorted(best_result['backlog_hours'])}")
        max_hour = best_result['hourly_stats'].idxmax()
        max_count = best_result['hourly_stats'].max()
        print(f"  最严重积压: {max_hour:02d}:00-{max_hour+1:02d}:00 ({max_count}班)")
    
    if backlog_patterns and backlog_patterns['continuous_periods']:
        print(f"\n【连续积压时段】")
        for i, period in enumerate(backlog_patterns['continuous_periods'], 1):
            start, end = period[0], period[-1]
            duration = len(period)
            print(f"  连续积压{i}: {start:02d}:00-{end+1:02d}:00 (持续{duration}小时)")
    
    print(f"\n【仿真建议】")
    print(f"  1. 延误判定: 采用起飞延误>15分钟标准")
    print(f"  2. 积压检测: 当某时段延误航班>10班时触发积压状态")
    print(f"  3. 重点监控: 早高峰(07:00-09:00)和晚高峰时段")
    print(f"  4. 数据清洗: 建议剔除地面滑行时间>60分钟的异常数据")

def main():
    """主函数"""
    print("=== ZGGG起飞延误精准积压分析 ===")
    
    # 1. 载入和预处理数据
    data = load_and_process_data()
    
    # 2. 分析延误特征
    delay_stats = analyze_delay_characteristics(data)
    
    # 3. 定义延误判定标准
    delay_criteria, normal_taxiing_threshold = define_delay_criteria(data)
    
    # 4. 识别积压时段
    backlog_results = identify_backlog_periods(delay_criteria, threshold=10)
    
    # 5. 分析积压模式
    backlog_patterns = analyze_backlog_patterns(backlog_results)
    
    # 6. 可视化结果
    visualize_results(data, backlog_results, backlog_patterns)
    
    # 7. 生成总结报告
    generate_summary_report(data, delay_criteria, backlog_results, backlog_patterns)
    
    return data, delay_criteria, backlog_results

if __name__ == "__main__":
    data, delay_criteria, backlog_results = main()
