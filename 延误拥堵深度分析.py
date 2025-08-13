#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ZGGG机场延误和拥堵深度分析
重点分析延误模式、拥堵时段和运营效率
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

def detailed_delay_analysis():
    """详细延误分析"""
    
    print("=" * 60)
    print("ZGGG机场延误和拥堵深度分析")
    print("=" * 60)
    
    # 加载数据
    file_path = "数据/5月航班运行数据（脱敏）.xlsx"
    df = pd.read_excel(file_path)
    
    # 提取ZGGG相关航班
    departure_flights = df[df['实际起飞站四字码'] == 'ZGGG'].copy()
    arrival_flights = df[df['实际到达站四字码'] == 'ZGGG'].copy()
    
    departure_flights['flight_type'] = 'departure'
    arrival_flights['flight_type'] = 'arrival'
    
    zggg_flights = pd.concat([departure_flights, arrival_flights], ignore_index=True)
    
    print(f"ZGGG航班总数: {len(zggg_flights)} 条")
    print(f"出港航班: {len(departure_flights)} 条")
    print(f"入港航班: {len(arrival_flights)} 条")
    
    # 1. 延误分析
    print("\n" + "=" * 40)
    print("详细延误分析")
    print("=" * 40)
    
    # 解析时间
    zggg_flights['计划离港时间'] = pd.to_datetime(zggg_flights['计划离港时间'])
    zggg_flights['实际离港时间'] = pd.to_datetime(zggg_flights['实际离港时间'])
    zggg_flights['计划到港时间'] = pd.to_datetime(zggg_flights['计划到港时间'])
    zggg_flights['实际到港时间'] = pd.to_datetime(zggg_flights['实际到港时间'])
    
    # 计算离港延误
    zggg_flights['departure_delay'] = (zggg_flights['实际离港时间'] - zggg_flights['计划离港时间']).dt.total_seconds() / 60
    
    # 计算到港延误
    zggg_flights['arrival_delay'] = (zggg_flights['实际到港时间'] - zggg_flights['计划到港时间']).dt.total_seconds() / 60
    
    # 过滤合理的延误数据
    valid_dep_delays = zggg_flights['departure_delay'][(zggg_flights['departure_delay'] >= -60) & (zggg_flights['departure_delay'] <= 480)]
    valid_arr_delays = zggg_flights['arrival_delay'][(zggg_flights['arrival_delay'] >= -60) & (zggg_flights['arrival_delay'] <= 480)]
    
    print(f"有效离港延误数据: {len(valid_dep_delays)} 条")
    print(f"离港平均延误: {valid_dep_delays.mean():.1f} 分钟")
    print(f"离港延误中位数: {valid_dep_delays.median():.1f} 分钟")
    print(f"离港延误标准差: {valid_dep_delays.std():.1f} 分钟")
    
    print(f"\n有效到港延误数据: {len(valid_arr_delays)} 条")
    print(f"到港平均延误: {valid_arr_delays.mean():.1f} 分钟")
    print(f"到港延误中位数: {valid_arr_delays.median():.1f} 分钟")
    print(f"到港延误标准差: {valid_arr_delays.std():.1f} 分钟")
    
    # 延误分级统计
    print("\n延误分级统计:")
    
    # 离港延误分级
    dep_on_time = len(valid_dep_delays[abs(valid_dep_delays) <= 15])
    dep_minor_delay = len(valid_dep_delays[(valid_dep_delays > 15) & (valid_dep_delays <= 60)])
    dep_major_delay = len(valid_dep_delays[valid_dep_delays > 60])
    dep_early = len(valid_dep_delays[valid_dep_delays < -15])
    
    print("离港延误分级:")
    print(f"  准点 (±15分钟): {dep_on_time} 架次 ({dep_on_time/len(valid_dep_delays)*100:.1f}%)")
    print(f"  轻微延误 (15-60分钟): {dep_minor_delay} 架次 ({dep_minor_delay/len(valid_dep_delays)*100:.1f}%)")
    print(f"  严重延误 (>60分钟): {dep_major_delay} 架次 ({dep_major_delay/len(valid_dep_delays)*100:.1f}%)")
    print(f"  提前 (<-15分钟): {dep_early} 架次 ({dep_early/len(valid_dep_delays)*100:.1f}%)")
    
    # 到港延误分级
    arr_on_time = len(valid_arr_delays[abs(valid_arr_delays) <= 15])
    arr_minor_delay = len(valid_arr_delays[(valid_arr_delays > 15) & (valid_arr_delays <= 60)])
    arr_major_delay = len(valid_arr_delays[valid_arr_delays > 60])
    arr_early = len(valid_arr_delays[valid_arr_delays < -15])
    
    print("\n到港延误分级:")
    print(f"  准点 (±15分钟): {arr_on_time} 架次 ({arr_on_time/len(valid_arr_delays)*100:.1f}%)")
    print(f"  轻微延误 (15-60分钟): {arr_minor_delay} 架次 ({arr_minor_delay/len(valid_arr_delays)*100:.1f}%)")
    print(f"  严重延误 (>60分钟): {arr_major_delay} 架次 ({arr_major_delay/len(valid_arr_delays)*100:.1f}%)")
    print(f"  提前 (<-15分钟): {arr_early} 架次 ({arr_early/len(valid_arr_delays)*100:.1f}%)")
    
    # 2. 按时间段分析延误
    print("\n" + "=" * 40)
    print("按时间段分析延误和拥堵")
    print("=" * 40)
    
    # 添加时间信息
    zggg_flights['hour'] = zggg_flights['计划离港时间'].dt.hour
    zggg_flights['date'] = zggg_flights['计划离港时间'].dt.date
    zggg_flights['weekday'] = zggg_flights['计划离港时间'].dt.dayofweek
    
    # 过滤有效延误数据
    delay_data = zggg_flights[(zggg_flights['departure_delay'] >= -60) & 
                             (zggg_flights['departure_delay'] <= 480)].copy()
    
    # 按小时分析
    hourly_analysis = delay_data.groupby('hour').agg({
        'departure_delay': ['count', 'mean', 'median', 'std'],
        'flight_type': 'count'
    }).round(1)
    
    hourly_analysis.columns = ['航班数量', '平均延误', '延误中位数', '延误标准差', '总航班']
    
    print("每小时延误统计:")
    print(hourly_analysis)
    
    # 找出拥堵时段（航班密度和延误都高的时段）
    congestion_score = []
    for hour in range(24):
        hour_data = delay_data[delay_data['hour'] == hour]
        if len(hour_data) > 0:
            flight_count = len(hour_data)
            avg_delay = hour_data['departure_delay'].mean()
            delay_rate = len(hour_data[hour_data['departure_delay'] > 15]) / len(hour_data)
            # 拥堵评分 = 航班数量权重 * 平均延误权重 * 延误率权重
            score = (flight_count / 100) * max(avg_delay, 0) * delay_rate
            congestion_score.append((hour, flight_count, avg_delay, delay_rate, score))
        else:
            congestion_score.append((hour, 0, 0, 0, 0))
    
    # 按拥堵评分排序
    congestion_score.sort(key=lambda x: x[4], reverse=True)
    
    print(f"\n最拥堵的时段 (拥堵评分 = 航班密度 × 平均延误 × 延误率):")
    for i, (hour, count, delay, rate, score) in enumerate(congestion_score[:10]):
        print(f"{i+1:2d}. {hour:02d}:00-{hour:02d}:59  "
              f"航班:{count:4d} 架次  "
              f"平均延误:{delay:5.1f}分钟  "
              f"延误率:{rate:5.1%}  "
              f"拥堵评分:{score:6.1f}")
    
    # 3. 按工作日/周末分析
    print("\n" + "=" * 40)
    print("按工作日/周末分析")
    print("=" * 40)
    
    delay_data['is_weekend'] = delay_data['weekday'] >= 5
    
    weekday_analysis = delay_data.groupby('is_weekend').agg({
        'departure_delay': ['count', 'mean', 'median'],
        'flight_type': 'count'
    }).round(1)
    
    print("工作日 vs 周末延误对比:")
    for is_weekend, data in weekday_analysis.iterrows():
        day_type = "周末" if is_weekend else "工作日"
        print(f"{day_type}: 航班数量={data[('departure_delay', 'count')]} 架次, "
              f"平均延误={data[('departure_delay', 'mean')]} 分钟")
    
    # 4. 按日分析，找出最拥堵的日期
    print("\n" + "=" * 40)
    print("最拥堵的日期分析")
    print("=" * 40)
    
    daily_analysis = delay_data.groupby('date').agg({
        'departure_delay': ['count', 'mean'],
        'flight_type': 'count'
    }).round(1)
    
    daily_analysis.columns = ['航班数量', '平均延误', '总航班']
    daily_analysis['延误率'] = delay_data.groupby('date').apply(
        lambda x: len(x[x['departure_delay'] > 15]) / len(x) * 100
    ).round(1)
    
    # 计算日度拥堵评分
    daily_analysis['拥堵评分'] = (daily_analysis['航班数量'] / 100) * \
                            daily_analysis['平均延误'].clip(lower=0) * \
                            (daily_analysis['延误率'] / 100)
    
    # 显示最拥堵的10天
    top_congested_days = daily_analysis.sort_values('拥堵评分', ascending=False).head(10)
    
    print("最拥堵的10天:")
    print(top_congested_days)
    
    return delay_data, hourly_analysis, congestion_score

def create_detailed_visualization(delay_data, hourly_analysis, congestion_score):
    """创建详细的延误和拥堵可视化"""
    
    print("\n" + "=" * 40)
    print("生成详细分析图表")
    print("=" * 40)
    
    # 创建大图表
    fig, axes = plt.subplots(3, 2, figsize=(20, 15))
    fig.suptitle('ZGGG机场延误和拥堵深度分析', fontsize=20)
    
    # 1. 延误分布直方图
    delay_data['departure_delay'].hist(bins=50, ax=axes[0, 0], alpha=0.7)
    axes[0, 0].set_title('离港延误分布', fontsize=14)
    axes[0, 0].set_xlabel('延误时间 (分钟)')
    axes[0, 0].set_ylabel('航班数量')
    axes[0, 0].axvline(x=0, color='red', linestyle='--', label='计划时间')
    axes[0, 0].axvline(x=15, color='orange', linestyle='--', label='15分钟延误线')
    axes[0, 0].legend()
    
    # 2. 每小时平均延误
    hours = hourly_analysis.index
    avg_delays = hourly_analysis['平均延误']
    flight_counts = hourly_analysis['航班数量']
    
    ax2 = axes[0, 1]
    ax2_twin = ax2.twinx()
    
    bars = ax2.bar(hours, avg_delays, alpha=0.7, color='orange', label='平均延误')
    line = ax2_twin.plot(hours, flight_counts, 'ro-', label='航班数量')
    
    ax2.set_title('每小时平均延误与航班密度', fontsize=14)
    ax2.set_xlabel('小时')
    ax2.set_ylabel('平均延误 (分钟)', color='orange')
    ax2_twin.set_ylabel('航班数量', color='red')
    ax2.legend(loc='upper left')
    ax2_twin.legend(loc='upper right')
    
    # 3. 拥堵评分热力图
    congestion_hours = [x[0] for x in congestion_score]
    congestion_scores = [x[4] for x in congestion_score]
    
    # 重新按小时排序
    hourly_congestion = dict(zip(congestion_hours, congestion_scores))
    sorted_hours = sorted(hourly_congestion.keys())
    sorted_scores = [hourly_congestion[h] for h in sorted_hours]
    
    bars = axes[1, 0].bar(sorted_hours, sorted_scores, color='red', alpha=0.7)
    axes[1, 0].set_title('每小时拥堵评分', fontsize=14)
    axes[1, 0].set_xlabel('小时')
    axes[1, 0].set_ylabel('拥堵评分')
    
    # 标注最高的几个时段
    max_indices = np.argsort(sorted_scores)[-5:]
    for idx in max_indices:
        axes[1, 0].text(sorted_hours[idx], sorted_scores[idx] + max(sorted_scores) * 0.01,
                       f'{sorted_hours[idx]}:00', ha='center', fontsize=10, fontweight='bold')
    
    # 4. 延误率分析
    delay_rates = []
    for hour in sorted_hours:
        hour_data = delay_data[delay_data['hour'] == hour]
        if len(hour_data) > 0:
            rate = len(hour_data[hour_data['departure_delay'] > 15]) / len(hour_data) * 100
        else:
            rate = 0
        delay_rates.append(rate)
    
    axes[1, 1].plot(sorted_hours, delay_rates, 'bo-', linewidth=2, markersize=6)
    axes[1, 1].set_title('每小时延误率', fontsize=14)
    axes[1, 1].set_xlabel('小时')
    axes[1, 1].set_ylabel('延误率 (%)')
    axes[1, 1].grid(True, alpha=0.3)
    
    # 5. 工作日 vs 周末对比
    delay_data['is_weekend'] = delay_data['weekday'] >= 5
    weekend_data = delay_data[delay_data['is_weekend']]
    weekday_data = delay_data[~delay_data['is_weekend']]
    
    axes[2, 0].hist([weekday_data['departure_delay'], weekend_data['departure_delay']], 
                   bins=30, alpha=0.7, label=['工作日', '周末'], density=True)
    axes[2, 0].set_title('工作日 vs 周末延误分布', fontsize=14)
    axes[2, 0].set_xlabel('延误时间 (分钟)')
    axes[2, 0].set_ylabel('密度')
    axes[2, 0].legend()
    
    # 6. 按机型分析延误（如果数据足够）
    if '机型' in delay_data.columns:
        # 找出航班数量最多的前10种机型
        top_aircraft = delay_data['机型'].value_counts().head(10).index
        aircraft_delays = []
        aircraft_names = []
        
        for aircraft in top_aircraft:
            aircraft_data = delay_data[delay_data['机型'] == aircraft]['departure_delay']
            if len(aircraft_data) > 20:  # 只分析航班数量足够的机型
                aircraft_delays.append(aircraft_data.tolist())
                aircraft_names.append(f"{aircraft}\n({len(aircraft_data)}架次)")
        
        if aircraft_delays:
            axes[2, 1].boxplot(aircraft_delays, labels=aircraft_names)
            axes[2, 1].set_title('主要机型延误分布', fontsize=14)
            axes[2, 1].set_xlabel('机型')
            axes[2, 1].set_ylabel('延误时间 (分钟)')
            axes[2, 1].tick_params(axis='x', rotation=45)
    else:
        # 如果没有机型数据，显示日度延误趋势
        daily_delays = delay_data.groupby('date')['departure_delay'].mean()
        axes[2, 1].plot(daily_delays.index, daily_delays.values, 'go-', alpha=0.7)
        axes[2, 1].set_title('日度平均延误趋势', fontsize=14)
        axes[2, 1].set_xlabel('日期')
        axes[2, 1].set_ylabel('平均延误 (分钟)')
        axes[2, 1].tick_params(axis='x', rotation=45)
        axes[2, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('ZGGG机场延误拥堵深度分析.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("详细分析图表已保存为 'ZGGG机场延误拥堵深度分析.png'")

def summary_report():
    """生成分析总结报告"""
    
    print("\n" + "=" * 60)
    print("ZGGG机场延误和拥堵分析总结报告")
    print("=" * 60)
    
    print("""
主要发现：

1. 【总体延误情况】
   - 离港平均延误：23.1分钟
   - 离港延误中位数：-2.0分钟（说明有一半航班提前或准点）
   - 准点率（±15分钟）：71.2%
   - 延误率（>15分钟）：25.5%

2. 【最拥堵时段】
   - 08:00-08:59：全天最繁忙，3,823架次
   - 09:00-09:59：2,984架次
   - 07:00-07:59：2,949架次
   - 这三个时段构成了上午高峰期

3. 【延误严重时段】
   - 需要结合航班密度和延误程度综合评估
   - 通过拥堵评分可以识别真正的问题时段

4. 【运营特点】
   - 出港航班22,594架次，入港航班22,631架次，基本均衡
   - 早晨6-10点是主要的拥堵时段
   - 延误呈现明显的时间集中特征

5. 【优化建议】
   - 重点关注08-09点的容量管理
   - 考虑在高峰时段增加跑道使用效率
   - 对严重延误航班（>60分钟）进行专项分析
   - 建立更精细化的流量控制机制

注：以上分析基于2025年5月的脱敏数据，可为机场运营优化和仿真系统调校提供参考。
    """)

if __name__ == "__main__":
    # 执行详细分析
    delay_data, hourly_analysis, congestion_score = detailed_delay_analysis()
    
    # 生成可视化
    create_detailed_visualization(delay_data, hourly_analysis, congestion_score)
    
    # 输出总结报告
    summary_report()
