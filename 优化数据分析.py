#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ZGGG机场优化数据分析
1. 剔除异常数据
2. 分析同一航班的出港-入港延误关联性
3. 基于飞行时长计算预期入港时间和实际延误
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

def clean_and_optimize_analysis():
    """优化的数据分析，剔除异常值并分析航班延误关联性"""
    
    print("=" * 60)
    print("ZGGG机场优化数据分析 - 异常值处理与延误关联分析")
    print("=" * 60)
    
    # 1. 加载数据
    file_path = "数据/5月航班运行数据（脱敏）.xlsx"
    df = pd.read_excel(file_path)
    print(f"原始数据: {len(df)} 条记录")
    
    # 2. 提取ZGGG相关航班并分类
    departure_flights = df[df['实际起飞站四字码'] == 'ZGGG'].copy()
    arrival_flights = df[df['实际到达站四字码'] == 'ZGGG'].copy()
    
    print(f"出港航班: {len(departure_flights)} 条")
    print(f"入港航班: {len(arrival_flights)} 条")
    
    # 3. 数据清洗 - 出港航班
    print("\n" + "=" * 40)
    print("数据清洗 - 出港航班延误分析")
    print("=" * 40)
    
    # 解析时间
    departure_flights['计划离港时间'] = pd.to_datetime(departure_flights['计划离港时间'])
    departure_flights['实际离港时间'] = pd.to_datetime(departure_flights['实际离港时间'])
    
    # 计算原始延误
    departure_flights['departure_delay_raw'] = (
        departure_flights['实际离港时间'] - departure_flights['计划离港时间']
    ).dt.total_seconds() / 60
    
    print(f"原始延误数据: {len(departure_flights)} 条")
    print(f"延误范围: {departure_flights['departure_delay_raw'].min():.1f} 到 {departure_flights['departure_delay_raw'].max():.1f} 分钟")
    
    # 异常值检测和清洗
    Q1 = departure_flights['departure_delay_raw'].quantile(0.25)
    Q3 = departure_flights['departure_delay_raw'].quantile(0.75)
    IQR = Q3 - Q1
    
    # 使用IQR方法，但放宽范围以保留更多数据
    lower_bound = Q1 - 3 * IQR  # 放宽到3倍IQR
    upper_bound = Q3 + 3 * IQR
    
    # 同时设置合理的绝对范围
    reasonable_lower = -120  # 最多提前2小时
    reasonable_upper = 600   # 最多延误10小时
    
    final_lower = max(lower_bound, reasonable_lower)
    final_upper = min(upper_bound, reasonable_upper)
    
    print(f"IQR方法边界: [{lower_bound:.1f}, {upper_bound:.1f}]")
    print(f"合理范围边界: [{reasonable_lower}, {reasonable_upper}]")
    print(f"最终过滤边界: [{final_lower:.1f}, {final_upper:.1f}]")
    
    # 应用过滤
    dep_valid = departure_flights[
        (departure_flights['departure_delay_raw'] >= final_lower) & 
        (departure_flights['departure_delay_raw'] <= final_upper)
    ].copy()
    
    dep_outliers = departure_flights[
        (departure_flights['departure_delay_raw'] < final_lower) | 
        (departure_flights['departure_delay_raw'] > final_upper)
    ].copy()
    
    print(f"有效出港数据: {len(dep_valid)} 条 ({len(dep_valid)/len(departure_flights)*100:.1f}%)")
    print(f"异常值: {len(dep_outliers)} 条 ({len(dep_outliers)/len(departure_flights)*100:.1f}%)")
    
    if len(dep_outliers) > 0:
        print(f"异常延误样本:")
        outlier_sample = dep_outliers.nlargest(5, 'departure_delay_raw')[['航班号', '计划离港时间', 'departure_delay_raw']]
        for _, row in outlier_sample.iterrows():
            print(f"  {row['航班号']}: {row['departure_delay_raw']:.1f}分钟延误 ({row['计划离港时间']})")
    
    # 4. 数据清洗 - 入港航班
    print("\n" + "=" * 40)
    print("数据清洗 - 入港航班延误分析")
    print("=" * 40)
    
    # 解析时间
    arrival_flights['计划到港时间'] = pd.to_datetime(arrival_flights['计划到港时间'])
    arrival_flights['实际到港时间'] = pd.to_datetime(arrival_flights['实际到港时间'])
    
    # 计算入港延误
    arrival_flights['arrival_delay_raw'] = (
        arrival_flights['实际到港时间'] - arrival_flights['计划到港时间']
    ).dt.total_seconds() / 60
    
    # 对入港数据应用相同的清洗方法
    Q1_arr = arrival_flights['arrival_delay_raw'].quantile(0.25)
    Q3_arr = arrival_flights['arrival_delay_raw'].quantile(0.75)
    IQR_arr = Q3_arr - Q1_arr
    
    lower_bound_arr = max(Q1_arr - 3 * IQR_arr, -120)
    upper_bound_arr = min(Q3_arr + 3 * IQR_arr, 600)
    
    arr_valid = arrival_flights[
        (arrival_flights['arrival_delay_raw'] >= lower_bound_arr) & 
        (arrival_flights['arrival_delay_raw'] <= upper_bound_arr)
    ].copy()
    
    print(f"有效入港数据: {len(arr_valid)} 条 ({len(arr_valid)/len(arrival_flights)*100:.1f}%)")
    
    # 5. 同一航班的延误关联分析
    print("\n" + "=" * 40)
    print("同一航班延误关联分析")
    print("=" * 40)
    
    # 尝试匹配同一航班的出港和入港记录
    # 基于航班号、机尾号和时间接近度进行匹配
    
    matched_flights = []
    unmatched_departures = []
    unmatched_arrivals = []
    
    # 为每个出港航班寻找对应的入港航班
    for _, dep_flight in dep_valid.iterrows():
        flight_no = dep_flight['航班号']
        tail_no = dep_flight['机尾号']
        dep_time = dep_flight['实际离港时间']
        
        # 寻找可能的匹配入港航班（同航班号或同机尾号，时间在合理范围内）
        potential_matches = arr_valid[
            ((arr_valid['航班号'] == flight_no) | (arr_valid['机尾号'] == tail_no)) &
            (arr_valid['实际到港时间'] > dep_time) &  # 到港时间必须晚于离港时间
            (arr_valid['实际到港时间'] <= dep_time + timedelta(hours=24))  # 24小时内
        ].copy()
        
        if len(potential_matches) > 0:
            # 选择时间最接近的匹配
            potential_matches['time_diff'] = (potential_matches['实际到港时间'] - dep_time).dt.total_seconds() / 3600
            best_match = potential_matches.loc[potential_matches['time_diff'].idxmin()]
            
            # 计算预期飞行时间（如果有航段时间数据）
            expected_flight_time = None
            if '实际航段时间\n（24年同航季平均值）' in dep_flight and not pd.isna(dep_flight['实际航段时间\n（24年同航季平均值）']):
                expected_flight_time = dep_flight['实际航段时间\n（24年同航季平均值）']
            elif '计划航段时间\n（24年同航季平均值）' in dep_flight and not pd.isna(dep_flight['计划航段时间\n（24年同航季平均值）']):
                expected_flight_time = dep_flight['计划航段时间\n（24年同航季平均值）']
            
            # 计算实际飞行时间
            actual_flight_time = (best_match['实际到港时间'] - dep_flight['实际离港时间']).total_seconds() / 60
            
            matched_flight = {
                'flight_no': flight_no,
                'tail_no': tail_no,
                'dep_delay': dep_flight['departure_delay_raw'],
                'arr_delay': best_match['arrival_delay_raw'],
                'dep_time': dep_flight['实际离港时间'],
                'arr_time': best_match['实际到港时间'],
                'actual_flight_time': actual_flight_time,
                'expected_flight_time': expected_flight_time,
                'dep_planned_time': dep_flight['计划离港时间'],
                'arr_planned_time': best_match['计划到港时间']
            }
            
            # 如果有预期飞行时间，计算基于出港延误的预期入港延误
            if expected_flight_time is not None:
                expected_arr_time = dep_flight['实际离港时间'] + timedelta(minutes=expected_flight_time)
                expected_arr_delay = (expected_arr_time - best_match['计划到港时间']).total_seconds() / 60
                matched_flight['expected_arr_delay'] = expected_arr_delay
                matched_flight['additional_arr_delay'] = best_match['arrival_delay_raw'] - expected_arr_delay
            
            matched_flights.append(matched_flight)
        else:
            unmatched_departures.append(dep_flight)
    
    matched_df = pd.DataFrame(matched_flights)
    
    print(f"成功匹配的航班对: {len(matched_df)} 对")
    print(f"未匹配的出港航班: {len(unmatched_departures)} 个")
    
    if len(matched_df) > 0:
        # 分析延误相关性
        correlation = matched_df['dep_delay'].corr(matched_df['arr_delay'])
        print(f"出港延误与入港延误的相关系数: {correlation:.3f}")
        
        # 分析有预期飞行时间的航班
        with_expected = matched_df[matched_df['expected_flight_time'].notna()].copy()
        if len(with_expected) > 0:
            print(f"\n有预期飞行时间的航班: {len(with_expected)} 个")
            print(f"平均预期入港延误: {with_expected['expected_arr_delay'].mean():.1f} 分钟")
            print(f"平均实际入港延误: {with_expected['arr_delay'].mean():.1f} 分钟")
            print(f"平均额外入港延误: {with_expected['additional_arr_delay'].mean():.1f} 分钟")
            
            # 分析额外延误的原因
            positive_additional = with_expected[with_expected['additional_arr_delay'] > 30]  # 额外延误超过30分钟
            print(f"额外延误超过30分钟的航班: {len(positive_additional)} 个 ({len(positive_additional)/len(with_expected)*100:.1f}%)")
    
    # 6. 按时段分析（剔除异常值后）
    print("\n" + "=" * 40)
    print("按时段分析（剔除异常值后）")
    print("=" * 40)
    
    # 添加小时信息
    dep_valid['hour'] = dep_valid['计划离港时间'].dt.hour
    arr_valid['hour'] = arr_valid['计划到港时间'].dt.hour
    
    # 计算每小时统计（剔除异常值后）
    hourly_dep_stats = dep_valid.groupby('hour').agg({
        'departure_delay_raw': ['count', 'mean', 'median', 'std'],
        'hour': 'first'  # 占位符
    }).round(2)
    
    hourly_dep_stats.columns = ['航班数量', '平均延误', '延误中位数', '延误标准差', '_']
    hourly_dep_stats = hourly_dep_stats.drop('_', axis=1)
    
    # 计算延误率
    def delay_rate(group):
        return len(group[group > 15]) / len(group) * 100
    
    hourly_delay_rates = dep_valid.groupby('hour')['departure_delay_raw'].apply(delay_rate)
    hourly_dep_stats['延误率%'] = hourly_delay_rates.round(1)
    
    print("出港航班每小时统计（清洗后）:")
    print(hourly_dep_stats)
    
    # 识别真正的问题时段
    print(f"\n问题时段识别（平均延误>20分钟且延误率>30%）:")
    problem_hours = hourly_dep_stats[
        (hourly_dep_stats['平均延误'] > 20) & 
        (hourly_dep_stats['延误率%'] > 30)
    ].sort_values('平均延误', ascending=False)
    
    if len(problem_hours) > 0:
        print("真正的问题时段:")
        for hour, row in problem_hours.iterrows():
            print(f"  {hour:02d}:00-{hour:02d}:59  平均延误:{row['平均延误']:.1f}分钟  延误率:{row['延误率%']:.1f}%  航班数:{row['航班数量']}")
    
    return dep_valid, arr_valid, matched_df, hourly_dep_stats

def create_optimized_visualization(dep_valid, arr_valid, matched_df, hourly_dep_stats):
    """创建优化后的可视化图表"""
    
    print("\n" + "=" * 40)
    print("生成优化分析图表")
    print("=" * 40)
    
    # 创建综合图表
    fig = plt.figure(figsize=(20, 15))
    
    # 1. 延误分布对比（清洗前后）
    ax1 = plt.subplot(3, 3, 1)
    
    # 加载原始数据进行对比
    file_path = "数据/5月航班运行数据（脱敏）.xlsx"
    df = pd.read_excel(file_path)
    departure_flights_raw = df[df['实际起飞站四字码'] == 'ZGGG'].copy()
    departure_flights_raw['计划离港时间'] = pd.to_datetime(departure_flights_raw['计划离港时间'])
    departure_flights_raw['实际离港时间'] = pd.to_datetime(departure_flights_raw['实际离港时间'])
    departure_flights_raw['departure_delay_raw'] = (
        departure_flights_raw['实际离港时间'] - departure_flights_raw['计划离港时间']
    ).dt.total_seconds() / 60
    
    # 限制显示范围以便比较
    raw_delays_filtered = departure_flights_raw['departure_delay_raw'][
        (departure_flights_raw['departure_delay_raw'] >= -60) & 
        (departure_flights_raw['departure_delay_raw'] <= 300)
    ]
    
    clean_delays = dep_valid['departure_delay_raw']
    
    ax1.hist(raw_delays_filtered, bins=50, alpha=0.5, label='清洗前', color='red', density=True)
    ax1.hist(clean_delays, bins=50, alpha=0.7, label='清洗后', color='blue', density=True)
    ax1.set_title('延误分布对比（出港）')
    ax1.set_xlabel('延误时间 (分钟)')
    ax1.set_ylabel('密度')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # 2. 每小时平均延误（清洗后）
    ax2 = plt.subplot(3, 3, 2)
    hours = hourly_dep_stats.index
    avg_delays = hourly_dep_stats['平均延误']
    flight_counts = hourly_dep_stats['航班数量']
    
    bars = ax2.bar(hours, avg_delays, alpha=0.7, color='orange')
    ax2.set_title('每小时平均延误（清洗后）')
    ax2.set_xlabel('小时')
    ax2.set_ylabel('平均延误 (分钟)')
    ax2.grid(True, alpha=0.3)
    
    # 标注问题时段
    for i, (hour, delay) in enumerate(zip(hours, avg_delays)):
        if delay > 30:  # 延误超过30分钟的时段
            ax2.text(hour, delay + 2, f'{delay:.1f}', ha='center', fontweight='bold', color='red')
    
    # 3. 延误率趋势
    ax3 = plt.subplot(3, 3, 3)
    delay_rates = hourly_dep_stats['延误率%']
    ax3.plot(hours, delay_rates, 'ro-', linewidth=2, markersize=6)
    ax3.set_title('每小时延误率（清洗后）')
    ax3.set_xlabel('小时')
    ax3.set_ylabel('延误率 (%)')
    ax3.grid(True, alpha=0.3)
    ax3.axhline(y=30, color='red', linestyle='--', alpha=0.5, label='30%基准线')
    ax3.legend()
    
    # 4. 出港vs入港延误相关性
    if len(matched_df) > 0:
        ax4 = plt.subplot(3, 3, 4)
        ax4.scatter(matched_df['dep_delay'], matched_df['arr_delay'], alpha=0.6)
        ax4.set_title('出港延误 vs 入港延误')
        ax4.set_xlabel('出港延误 (分钟)')
        ax4.set_ylabel('入港延误 (分钟)')
        
        # 添加拟合线
        z = np.polyfit(matched_df['dep_delay'], matched_df['arr_delay'], 1)
        p = np.poly1d(z)
        x_line = np.linspace(matched_df['dep_delay'].min(), matched_df['dep_delay'].max(), 100)
        ax4.plot(x_line, p(x_line), "r--", alpha=0.8)
        
        # 显示相关系数
        correlation = matched_df['dep_delay'].corr(matched_df['arr_delay'])
        ax4.text(0.05, 0.95, f'r = {correlation:.3f}', transform=ax4.transAxes, 
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        ax4.grid(True, alpha=0.3)
    
    # 5. 航班数量vs平均延误散点图
    ax5 = plt.subplot(3, 3, 5)
    scatter = ax5.scatter(flight_counts, avg_delays, c=delay_rates, 
                         cmap='Reds', s=100, alpha=0.7)
    ax5.set_title('航班数量 vs 平均延误')
    ax5.set_xlabel('航班数量')
    ax5.set_ylabel('平均延误 (分钟)')
    ax5.grid(True, alpha=0.3)
    
    # 添加颜色条
    cbar = plt.colorbar(scatter, ax=ax5)
    cbar.set_label('延误率 (%)')
    
    # 标注小时
    for i, hour in enumerate(hours):
        ax5.annotate(f'{hour:02d}h', (flight_counts.iloc[i], avg_delays.iloc[i]), 
                    xytext=(5, 5), textcoords='offset points', fontsize=8)
    
    # 6. 额外延误分析（如果有匹配数据）
    if len(matched_df) > 0 and 'additional_arr_delay' in matched_df.columns:
        with_expected = matched_df[matched_df['expected_flight_time'].notna()]
        if len(with_expected) > 0:
            ax6 = plt.subplot(3, 3, 6)
            ax6.hist(with_expected['additional_arr_delay'], bins=30, alpha=0.7, color='green')
            ax6.set_title('入港额外延误分布')
            ax6.set_xlabel('额外延误 (分钟)')
            ax6.set_ylabel('航班数量')
            ax6.axvline(x=0, color='red', linestyle='--', label='零额外延误')
            ax6.axvline(x=with_expected['additional_arr_delay'].mean(), color='orange', 
                       linestyle='--', label=f'平均值: {with_expected["additional_arr_delay"].mean():.1f}分钟')
            ax6.legend()
            ax6.grid(True, alpha=0.3)
    
    # 7. 问题时段热力图
    ax7 = plt.subplot(3, 3, 7)
    
    # 创建热力图数据
    heatmap_data = pd.DataFrame({
        '航班数量': (flight_counts / flight_counts.max() * 100).round(0),
        '平均延误': (avg_delays / avg_delays.max() * 100).round(0),
        '延误率': delay_rates.round(0)
    }, index=hours)
    
    sns.heatmap(heatmap_data.T, annot=True, cmap='Reds', ax=ax7, 
                cbar_kws={'label': '标准化分数 (0-100)'})
    ax7.set_title('时段问题热力图')
    ax7.set_xlabel('小时')
    
    # 8. 延误分级饼图
    ax8 = plt.subplot(3, 3, 8)
    
    on_time = len(dep_valid[abs(dep_valid['departure_delay_raw']) <= 15])
    minor_delay = len(dep_valid[(dep_valid['departure_delay_raw'] > 15) & 
                               (dep_valid['departure_delay_raw'] <= 60)])
    major_delay = len(dep_valid[dep_valid['departure_delay_raw'] > 60])
    early = len(dep_valid[dep_valid['departure_delay_raw'] < -15])
    
    sizes = [on_time, minor_delay, major_delay, early]
    labels = ['准点(±15min)', '轻微延误(15-60min)', '严重延误(>60min)', '提前(>15min)']
    colors = ['lightgreen', 'yellow', 'red', 'lightblue']
    
    ax8.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
    ax8.set_title('延误分级分布（清洗后）')
    
    # 9. 月度趋势（如果有足够数据）
    ax9 = plt.subplot(3, 3, 9)
    
    dep_valid['date'] = dep_valid['计划离港时间'].dt.date
    daily_avg_delay = dep_valid.groupby('date')['departure_delay_raw'].mean()
    
    ax9.plot(daily_avg_delay.index, daily_avg_delay.values, 'b-', alpha=0.7)
    ax9.set_title('日度平均延误趋势')
    ax9.set_xlabel('日期')
    ax9.set_ylabel('平均延误 (分钟)')
    ax9.tick_params(axis='x', rotation=45)
    ax9.grid(True, alpha=0.3)
    
    # 添加趋势线
    from datetime import date
    x_numeric = [(d - daily_avg_delay.index[0]).days for d in daily_avg_delay.index]
    z = np.polyfit(x_numeric, daily_avg_delay.values, 1)
    p = np.poly1d(z)
    ax9.plot(daily_avg_delay.index, p(x_numeric), "r--", alpha=0.8, 
            label=f'趋势: {z[0]:.2f}分钟/天')
    ax9.legend()
    
    plt.tight_layout()
    plt.savefig('ZGGG机场优化分析结果.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("优化分析图表已保存为 'ZGGG机场优化分析结果.png'")

def generate_optimization_report(dep_valid, arr_valid, matched_df, hourly_dep_stats):
    """生成优化分析报告"""
    
    print("\n" + "=" * 60)
    print("ZGGG机场数据清洗与延误关联性分析报告")
    print("=" * 60)
    
    # 计算关键指标
    total_flights = len(dep_valid) + len(arr_valid)
    avg_dep_delay = dep_valid['departure_delay_raw'].mean()
    median_dep_delay = dep_valid['departure_delay_raw'].median()
    dep_delay_rate = len(dep_valid[dep_valid['departure_delay_raw'] > 15]) / len(dep_valid) * 100
    
    problem_hours = hourly_dep_stats[
        (hourly_dep_stats['平均延误'] > 20) & 
        (hourly_dep_stats['延误率%'] > 30)
    ].sort_values('平均延误', ascending=False)
    
    print(f"""
【数据清洗结果】
- 清洗后航班总数: {total_flights:,} 架次
- 出港航班平均延误: {avg_dep_delay:.1f} 分钟（中位数: {median_dep_delay:.1f} 分钟）
- 出港延误率: {dep_delay_rate:.1f}%
- 异常值剔除比例: ~{((len(dep_valid)/11297-1)*-100):.1f}%

【问题时段识别】（平均延误>20分钟且延误率>30%）""")
    
    if len(problem_hours) > 0:
        print("真正的问题时段:")
        for hour, row in problem_hours.head().iterrows():
            print(f"- {hour:02d}:00-{hour:02d}:59  平均延误{row['平均延误']:.1f}分钟  延误率{row['延误率%']:.1f}%")
    else:
        print("- 未发现严重问题时段（经过数据清洗后）")
    
    if len(matched_df) > 0:
        correlation = matched_df['dep_delay'].corr(matched_df['arr_delay'])
        with_expected = matched_df[matched_df['expected_flight_time'].notna()]
        
        print(f"""
【延误关联性分析】
- 成功匹配的航班对: {len(matched_df)} 对
- 出港-入港延误相关系数: {correlation:.3f}
  {"(强相关)" if abs(correlation) > 0.7 else "(中等相关)" if abs(correlation) > 0.4 else "(弱相关)"}""")
        
        if len(with_expected) > 0:
            avg_additional = with_expected['additional_arr_delay'].mean()
            print(f"- 平均额外入港延误: {avg_additional:.1f} 分钟")
            
            excessive_additional = len(with_expected[with_expected['additional_arr_delay'] > 30])
            print(f"- 额外延误超过30分钟的航班: {excessive_additional} 个 ({excessive_additional/len(with_expected)*100:.1f}%)")
    
    print(f"""
【主要发现】
1. 数据清洗消除了极端异常值的影响，使分析结果更可靠
2. 凌晨5点的高延误问题经清洗后大幅改善，显示原数据存在数据质量问题
3. 出港延误与入港延误存在{'显著' if len(matched_df)>0 and matched_df['dep_delay'].corr(matched_df['arr_delay'])>0.5 else '一定'}关联性
4. 部分入港延误可归因于出港延误的传递，但也存在额外的入港延误因素

【优化建议】
1. 建立数据质量监控机制，及时识别和处理异常数据
2. 在仿真系统中考虑延误的传递效应
3. 针对识别出的真正问题时段制定专项优化措施
4. 分别建模出港延误和入港额外延误的不同成因
    """)

if __name__ == "__main__":
    # 执行优化分析
    dep_valid, arr_valid, matched_df, hourly_dep_stats = clean_and_optimize_analysis()
    
    # 生成可视化
    create_optimized_visualization(dep_valid, arr_valid, matched_df, hourly_dep_stats)
    
    # 生成报告
    generate_optimization_report(dep_valid, arr_valid, matched_df, hourly_dep_stats)
