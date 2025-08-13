#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG机场起飞仿真与实际对比综合分析系统

本文件整合了ZGGG机场的起飞仿真系统与仿真-现实对比分析模块。

**V4 Optimization Notes:**
- 用户反馈V3版仍有“滞后效应”，且所有航班均被延误，部分延误时长不切实际。
- 核心问题：仿真模型过于刚性，导致延误无限累积，无法体现真实运行中的效率恢复。
- V4优化策略：
  1. **更积极地缩短有效滑行时间**: 大幅降低`TAXI_OUT_MULTIPLIER`，从根本上减少每个航班的基础延误，力求让部分航班实现低延误或零延误。
  2. **进一步提升机场运行效率**: 再次降低ROT和尾流间隔，提高小时出港极限，加速积压的消散。
  3. **增加诊断输出**: 在延误分析中加入低延误航班的统计，并对不切实际的超长延误发出警告。
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
import os

warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False


class ZGGGDepartureSimulator:
    """ ZGGG机场飞机起飞仿真系统 """
    def __init__(self, taxi_out_time=15, taxi_out_multiplier=1.0):
        self.base_taxi_out_time = taxi_out_time
        self.taxi_out_multiplier = taxi_out_multiplier
        self.effective_taxi_out_time = self.base_taxi_out_time * self.taxi_out_multiplier
        self.data = None
        self.weather_suspended_periods = []
        self.aircraft_categories = {}
        self.wake_separation_matrix = {}

        print("=== ZGGG起飞仿真器初始化 (V4) ===")
        print(f"基础Taxi-out时间: {self.base_taxi_out_time} 分钟")
        print(f"滑行时间乘数: {self.taxi_out_multiplier}")
        print(f"--> V4有效Taxi-out时间: {self.effective_taxi_out_time:.1f} 分钟")

    def load_departure_data(self, file_path):
        """载入ZGGG起飞航班数据"""
        print("\n=== 载入ZGGG起飞航班数据 ===")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"数据文件未找到: {file_path}")
        df = pd.read_excel(file_path)
        zggg_departures = df[df['实际起飞站四字码'] == 'ZGGG'].copy()
        time_fields = ['计划离港时间', '实际离港时间', '实际起飞时间']
        for field in time_fields:
            zggg_departures[field] = pd.to_datetime(zggg_departures[field], errors='coerce')
        
        required_fields = ['航班号', '机型', '唯一序列号', '计划离港时间', '实际离港时间']
        valid_flights = zggg_departures.dropna(subset=required_fields).copy()
        
        complete_flights = valid_flights[valid_flights['实际起飞时间'].notna()].copy()
        missing_takeoff = valid_flights[valid_flights['实际起飞时间'].isna()].copy()
        
        if not missing_takeoff.empty:
            missing_takeoff['实际起飞时间'] = missing_takeoff['实际离港时间'] + pd.Timedelta(minutes=20)
        
        all_flights = pd.concat([complete_flights, missing_takeoff], ignore_index=True)
        all_flights = all_flights[all_flights['实际离港时间'] <= all_flights['实际起飞时间']].copy()
        
        all_flights['起飞延误分钟'] = (all_flights['实际起飞时间'] - all_flights['计划离港时间']).dt.total_seconds() / 60
        self.data = all_flights
        print(f"数据清洗与预处理完成，可用航班数: {len(self.data)}")
        return self.data

    def classify_aircraft_types(self):
        """机型分类和ROT、尾流参数设定"""
        print("\n=== 机型分类和ROT/尾流参数设定 (V4 优化) ===")
        # V4优化：进一步减少ROT和尾流间隔，提升机场运行效率极限
        self.aircraft_categories = {
            'Heavy': {'types': ['773', '772', '77W', '77L', '744', '748', '380', '359', '358', '35K'], 'rot_seconds': 95, 'wake_category': 'Heavy'},
            'Medium': {'types': ['32G', '32N', '32A', '321', '320', '319', '327', '32S', '32Q', '73M', '738', '739', '73G', '73H', '737', '73W', '73J', '909', '290', 'E90', 'ER4', 'ERJ', 'E75'], 'rot_seconds': 75, 'wake_category': 'Medium'},
            'Light': {'types': ['AT7', 'AT5', 'DH8', 'CR9', 'CRJ', 'CR7', 'E45', 'SF3', 'J41'], 'rot_seconds': 60, 'wake_category': 'Light'},
            'Cargo': {'types': ['76F', '77F', '74F', '32P', '737F'], 'rot_seconds': 105, 'wake_category': 'Heavy'}
        }
        
        def get_aircraft_category(aircraft_type):
            for category, info in self.aircraft_categories.items():
                if aircraft_type in info['types']:
                    return category, info['rot_seconds'], info['wake_category']
            return 'Medium', 75, 'Medium'
        
        self.data[['机型类别', 'ROT秒', '尾流类别']] = self.data['机型'].apply(lambda x: pd.Series(get_aircraft_category(x)))
        print("机型分类及ROT设定完成。")
        
        self.wake_separation_matrix = {
            ('Heavy', 'Heavy'): 95, ('Heavy', 'Medium'): 125, ('Heavy', 'Light'): 180,
            ('Medium', 'Heavy'): 70, ('Medium', 'Medium'): 90, ('Medium', 'Light'): 120,
            ('Light', 'Heavy'): 70, ('Light', 'Medium'): 70, ('Light', 'Light'): 90
        }
        print("尾流间隔矩阵设定完成。")

    def simulate_runway_queue(self, target_date):
        print(f"\n=== 开始对 {target_date} 进行跑道排队仿真 ===")
        target_datetime = pd.to_datetime(target_date)
        day_flights = self.data[self.data['计划离港时间'].dt.date == target_datetime.date()].copy().sort_values('计划离港时间')
        
        if day_flights.empty:
            return pd.DataFrame()
            
        runway_last_departure = {'02R/20L': None, '02L/20R': None}
        results = []

        for _, flight in day_flights.iterrows():
            selected_runway = np.random.choice(list(runway_last_departure.keys()))
            
            base_departure_time = flight['计划离港时间'] + pd.Timedelta(minutes=self.effective_taxi_out_time)
            
            earliest_takeoff = base_departure_time
            
            if runway_last_departure[selected_runway] is not None:
                last_flight = runway_last_departure[selected_runway]
                wake_key = (last_flight['尾流类别'], flight['尾流类别'])
                wake_separation = self.wake_separation_matrix.get(wake_key, 120)
                previous_rot = last_flight.get('ROT秒', 75)
                
                runway_free_time = last_flight['仿真起飞时间'] + pd.Timedelta(seconds=wake_separation + previous_rot)
                earliest_takeoff = max(base_departure_time, runway_free_time)
            
            simulated_takeoff = earliest_takeoff
            delay_minutes = (simulated_takeoff - flight['计划离港时间']).total_seconds() / 60
            
            results.append({'航班号': flight['航班号'], '计划起飞': flight['计划离港时间'],
                            '仿真起飞时间': simulated_takeoff, '仿真延误分钟': delay_minutes,
                            '尾流类别': flight['尾流类别'], 'ROT秒': flight['ROT秒']})
            
            runway_last_departure[selected_runway] = results[-1]
        
        print("仿真完成。")
        return pd.DataFrame(results)

class SimulationRealityComparator:
    """ 仿真与现实对比分析器 """
    def __init__(self, delay_threshold, backlog_threshold, taxi_out_time, taxi_out_multiplier, file_path):
        self.delay_threshold = delay_threshold
        self.backlog_threshold = backlog_threshold
        self.file_path = file_path
        self.simulator = ZGGGDepartureSimulator(taxi_out_time=taxi_out_time, taxi_out_multiplier=taxi_out_multiplier)
        
        print("\n=== 仿真现实对比分析器初始化 ===")
        print(f"单航班延误判定阈值: > {delay_threshold} 分钟")
        print(f"积压时段判定阈值: >= {backlog_threshold} 延误航班/小时")

    def prepare_data(self):
        print("\n--- 步骤 1: 准备仿真及分析数据 ---")
        self.real_data = self.simulator.load_departure_data(self.file_path)
        self.simulator.classify_aircraft_types()

    def analyze_real_backlog_patterns_daily(self):
        print("\n--- 步骤 2: 分析真实数据积压模式 (逐日分析) ---")
        delayed_flights = self.real_data[self.real_data['起飞延误分钟'] > self.delay_threshold].copy()
        delayed_flights['date'] = delayed_flights['计划离港时间'].dt.date
        delayed_flights['hour'] = delayed_flights['计划离港时间'].dt.hour
        
        daily_hourly_counts = delayed_flights.groupby(['date', 'hour']).size().reset_index(name='count')
        real_backlogs = daily_hourly_counts[daily_hourly_counts['count'] >= self.backlog_threshold].copy()
        
        if real_backlogs.empty:
            return None, None
            
        daily_peaks = real_backlogs.loc[real_backlogs.groupby('date')['count'].idxmax()]
        most_severe_day_info = daily_peaks.loc[daily_peaks['count'].idxmax()]
        target_date = most_severe_day_info['date']
        
        print(f"真实数据中，积压最严重的日期为: {target_date}")
        target_day_backlog_info = real_backlogs[real_backlogs['date'] == target_date]
        return target_date, target_day_backlog_info

    def run_simulation_for_target_day(self, target_date):
        print(f"\n--- 步骤 3: 针对目标日期 {target_date} 运行仿真 ---")
        sim_results = self.simulator.simulate_runway_queue(target_date)
        
        if sim_results.empty:
            return None, None
            
        sim_delayed = sim_results[sim_results['仿真延误分钟'] > self.delay_threshold].copy()
        sim_delayed['hour'] = sim_delayed['计划起飞'].dt.hour
        
        sim_hourly_counts = sim_delayed.groupby('hour').size().reset_index(name='count')
        sim_backlog_info = sim_hourly_counts[sim_hourly_counts['count'] >= self.backlog_threshold].copy()
        
        return sim_results, sim_backlog_info

    def compare_day_to_day(self, target_date, real_backlog_info, sim_results, sim_backlog_info):
        print(f"\n--- 步骤 4: 对比 {target_date} 的真实与仿真结果 ---")
        
        # V4 新增：仿真航班延误时长深度分析
        print("\n【仿真航班延误时长分析】")
        sim_delays = sim_results['仿真延误分钟']
        low_delay_count = (sim_delays < 15).sum()
        total_flights = len(sim_results)
        print(f"  - 低延误航班 (<15分钟): {low_delay_count} / {total_flights} ({low_delay_count/total_flights*100:.1f}%)")
        print(f"  - 平均延误: {sim_delays.mean():.1f} 分钟")
        print(f"  - 中位延误: {sim_delays.median():.1f} 分钟")
        print(f"  - 最大延误: {sim_delays.max():.1f} 分钟")
        if sim_delays.max() > 240: # 4小时
            print("  - 警告: 仿真出现超长延误(>240分钟)，可能不符合实际情况。")

        real_hours = set(real_backlog_info['hour'])
        real_peak_count = real_backlog_info['count'].max() if not real_backlog_info.empty else 0
        
        sim_hours = set(sim_backlog_info['hour']) if not sim_backlog_info.empty else set()
        sim_peak_count = sim_backlog_info['count'].max() if not sim_backlog_info.empty else 0
        
        print("\n【积压时段对比】")
        print(f"  - 真实积压时段: {len(real_hours)} 小时。分布: {sorted(list(real_hours))}")
        print(f"  - 仿真积压时段: {len(sim_hours)} 小时。分布: {sorted(list(sim_hours))}")

        print("\n【积压高峰对比】")
        print(f"  - 真实积压最高峰: {real_peak_count} 班")
        print(f"  - 仿真积压最高峰: {sim_peak_count} 班")

        deviation = abs(sim_peak_count - real_peak_count) / real_peak_count * 100 if real_peak_count > 0 else float('inf')
        print(f"  - 峰值偏差: {deviation:.1f}%")
        
        self.visualize_comparison(target_date, real_backlog_info, sim_results, sim_backlog_info)

    def visualize_comparison(self, target_date, real_backlog_info, sim_results, sim_backlog_info):
        fig, axes = plt.subplots(2, 1, figsize=(12, 12), gridspec_kw={'height_ratios': [2, 1]})
        
        ax1 = axes[0]
        hours = range(24)
        real_counts = pd.Series(index=hours, data=0)
        if not real_backlog_info.empty:
            real_counts.update(real_backlog_info.set_index('hour')['count'])
        sim_counts = pd.Series(index=hours, data=0)
        if not sim_backlog_info.empty:
             sim_counts.update(sim_backlog_info.set_index('hour')['count'])
        
        x = np.arange(len(hours))
        width = 0.35
        ax1.bar(x - width/2, real_counts.values, width, label='真实延误航班数', color='royalblue')
        ax1.bar(x + width/2, sim_counts.values, width, label='仿真延误航班数', color='tomato')
        ax1.axhline(y=self.backlog_threshold, color='orange', linestyle='--', label=f'积压阈值 ({self.backlog_threshold}班/小时)')
        ax1.set_title(f'{target_date} 真实 vs 仿真 小时延误航班对比', fontsize=14)
        ax1.set_xticks(x); ax1.set_xticklabels(hours); ax1.legend()

        ax2 = axes[1]
        ax2.hist(sim_results['仿真延误分钟'], bins=30, color='mediumseagreen', alpha=0.8)
        ax2.set_title('仿真航班延误时长分布 (分钟)', fontsize=14)
        ax2.set_xlabel('延误时长 (分钟)'); ax2.set_ylabel('航班数量')
        
        plt.tight_layout()
        plt.savefig(f'ZGGG_{target_date}_Comparison_V4.png', dpi=300)
        plt.show()

def main():
    print("="*60)
    print("       ZGGG 机场起飞仿真与对比分析系统启动 (V4)")
    print("="*60)

    # --- 核心可调参数 ---
    INDIVIDUAL_FLIGHT_DELAY_THRESHOLD = 15
    BACKLOG_THRESHOLD_FLIGHTS_PER_HOUR = 10
    TAXI_OUT_TIME_MINUTES = 18
    # V4 优化：大幅降低乘数以显著减少基础延误，解决“全民延误”和“滞后”问题
    TAXI_OUT_MULTIPLIER = 0.80 # 建议在 0.6 ~ 0.8 之间调整
    
    DATA_FILE_PATH = '数据/5月航班运行数据（脱敏）.xlsx'
    
    try:
        comparator = SimulationRealityComparator(
            delay_threshold=INDIVIDUAL_FLIGHT_DELAY_THRESHOLD,
            backlog_threshold=BACKLOG_THRESHOLD_FLIGHTS_PER_HOUR,
            taxi_out_time=TAXI_OUT_TIME_MINUTES,
            taxi_out_multiplier=TAXI_OUT_MULTIPLIER,
            file_path=DATA_FILE_PATH
        )
        
        comparator.prepare_data()
        
        target_date, real_backlog_info = comparator.analyze_real_backlog_patterns_daily()
        if target_date is None: return

        sim_results, sim_backlog_info = comparator.run_simulation_for_target_day(target_date)
        if sim_results is None: return

        comparator.compare_day_to_day(target_date, real_backlog_info, sim_results, sim_backlog_info)

    except FileNotFoundError as e:
        print(f"\n错误：数据文件未找到 '{DATA_FILE_PATH}'。请检查路径。")
    except Exception as e:
        print(f"\n程序运行中发生未知错误: {e}")

    print("\n="*60); print("       分析流程结束"); print("="*60)

if __name__ == "__main__":
    main()