#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG仿真与实际情况对比分析
比较仿真推演的积压时段、持续时长与实际情况的一致性
验证仿真模型的准确性并进行参数调优
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 导入仿真器
from ZGGG起飞仿真系统 import ZGGGDepartureSimulator

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

class SimulationRealityComparator:
    def __init__(self, delay_threshold=15, backlog_threshold=10):
        """
        仿真与现实对比分析器初始化
        
        Args:
            delay_threshold: 延误判定阈值(分钟)
            backlog_threshold: 积压判定阈值(班次/小时)
        """
        self.delay_threshold = delay_threshold
        self.backlog_threshold = backlog_threshold
        self.real_data = None
        self.simulation_results = {}
        
        print(f"=== 仿真现实对比分析器初始化 ===")
        print(f"延误判定阈值: {delay_threshold} 分钟")
        print(f"积压判定阈值: {backlog_threshold} 班/小时")
    
    def load_real_data(self):
        """载入真实数据并分析积压情况"""
        print(f"\n=== 载入真实数据 ===")
        
        df = pd.read_excel('/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/5月航班运行数据（脱敏）.xlsx')
        
        # 提取ZGGG起飞航班
        zggg_dep = df[df['实际起飞站四字码'] == 'ZGGG'].copy()
        
        # 转换时间字段
        time_fields = ['计划离港时间', '实际离港时间', '实际起飞时间']
        for field in time_fields:
            zggg_dep[field] = pd.to_datetime(zggg_dep[field], errors='coerce')
        
        # 只保留有完整时间数据的航班
        self.real_data = zggg_dep.dropna(subset=time_fields).copy()
        
        # 计算延误时间
        self.real_data['起飞延误分钟'] = (
            self.real_data['实际起飞时间'] - self.real_data['计划离港时间']
        ).dt.total_seconds() / 60
        
        print(f"真实数据载入: {len(self.real_data)} 班航班")
        return self.real_data
    
    def analyze_real_backlog_patterns(self):
        """分析真实积压模式"""
        print(f"\n=== 分析真实积压模式 ===")
        
        # 识别延误航班(使用相同的阈值)
        delayed_flights = self.real_data[
            self.real_data['起飞延误分钟'] > self.delay_threshold
        ].copy()
        
        print(f"真实延误航班数: {len(delayed_flights)} 班 ({len(delayed_flights)/len(self.real_data)*100:.1f}%)")
        
        # 按小时统计延误航班数 - 修正：计算日均值而非总和
        delayed_flights['hour'] = delayed_flights['计划离港时间'].dt.hour
        delayed_flights['date'] = delayed_flights['计划离港时间'].dt.date
        
        # 先按日期和小时分组，然后计算平均值
        daily_hourly_delays = delayed_flights.groupby(['date', 'hour']).size().reset_index(name='count')
        hourly_delays = daily_hourly_delays.groupby('hour')['count'].mean()
        
        print(f"真实延误航班数(日均):")
        for hour in sorted(hourly_delays.index):
            print(f"  {hour:02d}:00-{hour+1:02d}:00: 平均 {hourly_delays[hour]:.1f} 班/天")
        
        # 识别积压时段 - 基于日均数据
        real_backlog_hours = hourly_delays[hourly_delays > self.backlog_threshold].index.tolist()
        
        print(f"真实积压时段: {len(real_backlog_hours)} 个")
        if real_backlog_hours:
            print(f"积压时段列表: {sorted(real_backlog_hours)}")
            max_hour = hourly_delays.idxmax()
            max_count = hourly_delays.max()
            print(f"最严重积压: {max_hour:02d}:00-{max_hour+1:02d}:00 (日均{max_count:.1f}班)")
        
        # 识别连续积压时段
        continuous_periods = self._find_continuous_periods(real_backlog_hours)
        
        print(f"\n真实连续积压时段: {len(continuous_periods)} 个")
        for i, period in enumerate(continuous_periods, 1):
            start, end = period[0], period[-1]
            duration = len(period)
            total_delays = sum([hourly_delays.get(h, 0) for h in period])
            print(f"  连续积压{i}: {start:02d}:00-{end+1:02d}:00 (持续{duration}小时, 日均{total_delays:.1f}班延误)")
        
        return {
            'hourly_delays': hourly_delays,  # 这里现在是日均数据
            'backlog_hours': real_backlog_hours,
            'continuous_periods': continuous_periods,
            'delayed_flights': delayed_flights,
            'max_hour': max_hour,
            'max_count': max_count  # 这里现在是日均最高峰
        }
    
    def run_simulation_analysis(self, test_dates=None):
        """运行仿真分析"""
        print(f"\n=== 运行仿真分析 ===")
        
        # 初始化仿真器 - 使用优化后的参数
        simulator = ZGGGDepartureSimulator(
            delay_threshold=self.delay_threshold,
            backlog_threshold=self.backlog_threshold,
            taxi_out_time=10  # 进一步优化taxi-out时间
        )
        
        # 载入数据
        simulator.load_departure_data()
        simulator.identify_weather_suspended_periods()
        simulator.classify_aircraft_types()
        simulator.separate_flight_types()
        
        # 如果未指定测试日期，选择多个典型日期
        if test_dates is None:
            daily_counts = simulator.data['计划离港时间'].dt.date.value_counts()
            # 选择航班数量不同的几个日期进行测试
            sorted_dates = daily_counts.sort_values(ascending=False)
            test_dates = [
                sorted_dates.index[0],  # 最繁忙日期
                sorted_dates.index[len(sorted_dates)//4],  # 75分位
                sorted_dates.index[len(sorted_dates)//2],  # 中位数
                sorted_dates.index[len(sorted_dates)*3//4], # 25分位
            ]
        
        simulation_results = {}
        
        for date in test_dates:
            print(f"\n仿真日期: {date}")
            sim_result = simulator.simulate_runway_queue(target_date=date, verbose=False)
            
            # 分析仿真结果的积压情况
            delayed_sim = sim_result[sim_result['仿真延误分钟'] > self.delay_threshold]
            
            if len(delayed_sim) > 0:
                delayed_sim['hour'] = delayed_sim['计划起飞'].dt.hour
                hourly_sim_delays = delayed_sim.groupby('hour').size()
                sim_backlog_hours = hourly_sim_delays[hourly_sim_delays > self.backlog_threshold].index.tolist()
                
                simulation_results[date] = {
                    'total_flights': len(sim_result),
                    'delayed_flights': len(delayed_sim),
                    'delay_rate': len(delayed_sim) / len(sim_result) * 100,
                    'hourly_delays': hourly_sim_delays,
                    'backlog_hours': sim_backlog_hours,
                    'avg_delay': sim_result['仿真延误分钟'].mean(),
                    'max_hourly_delay': hourly_sim_delays.max() if len(hourly_sim_delays) > 0 else 0
                }
                
                print(f"  延误航班: {len(delayed_sim)} 班 ({len(delayed_sim)/len(sim_result)*100:.1f}%)")
                print(f"  积压时段: {len(sim_backlog_hours)} 个: {sorted(sim_backlog_hours)}")
            else:
                simulation_results[date] = {
                    'total_flights': len(sim_result),
                    'delayed_flights': 0,
                    'delay_rate': 0,
                    'hourly_delays': pd.Series(),
                    'backlog_hours': [],
                    'avg_delay': sim_result['仿真延误分钟'].mean(),
                    'max_hourly_delay': 0
                }
        
        self.simulation_results = simulation_results
        return simulation_results
    
    def compare_results(self, real_analysis, simulation_results):
        """对比仿真与现实结果"""
        print(f"\n" + "="*60)
        print(f"                仿真与现实对比分析")
        print(f"="*60)
        
        # 1. 积压时段对比
        real_backlog_hours = set(real_analysis['backlog_hours'])
        
        # 汇总所有仿真日期的积压时段
        all_sim_backlog_hours = set()
        for date, result in simulation_results.items():
            all_sim_backlog_hours.update(result['backlog_hours'])
        
        # 计算重叠度
        overlap = real_backlog_hours.intersection(all_sim_backlog_hours)
        overlap_rate = len(overlap) / len(real_backlog_hours) * 100 if len(real_backlog_hours) > 0 else 0
        
        print(f"\n【积压时段对比】")
        print(f"  真实积压时段: {len(real_backlog_hours)} 个: {sorted(real_backlog_hours)}")
        print(f"  仿真积压时段: {len(all_sim_backlog_hours)} 个: {sorted(all_sim_backlog_hours)}")
        print(f"  重叠时段: {len(overlap)} 个: {sorted(overlap)}")
        print(f"  重叠率: {overlap_rate:.1f}%")
        
        # 2. 最高峰对比 - 修正为日均对比
        real_max_count = real_analysis['max_count']  # 现在是日均数据
        
        # 计算仿真的最高峰 - 直接使用单日数据
        sim_max_counts = [result['max_hourly_delay'] for result in simulation_results.values()]
        sim_avg_max = np.mean(sim_max_counts) if sim_max_counts else 0
        
        deviation = abs(sim_avg_max - real_max_count) / real_max_count * 100 if real_max_count > 0 else 100
        
        print(f"\n【最高峰积压对比(修正为日均对比)】")
        print(f"  真实日均最高峰: {real_max_count:.1f} 班 ({real_analysis['max_hour']:02d}:00-{real_analysis['max_hour']+1:02d}:00)")
        print(f"  仿真平均最高峰: {sim_avg_max:.1f} 班")
        print(f"  偏差: {deviation:.1f}% ({'符合' if deviation <= 15 else '不符合'}15%要求)")
        
        # 3. 连续积压时段对比
        real_continuous = real_analysis['continuous_periods']
        
        print(f"\n【连续积压时段对比】")
        print(f"  真实连续积压: {len(real_continuous)} 个")
        for i, period in enumerate(real_continuous, 1):
            duration = len(period)
            print(f"    连续积压{i}: {period[0]:02d}:00-{period[-1]+1:02d}:00 (持续{duration}小时)")
        
        # 4. 总体评估
        print(f"\n【仿真模型评估】")
        criteria_met = 0
        total_criteria = 2
        
        if overlap_rate >= 50:  # 积压时段重叠率>=50%
            print(f"  ✅ 积压时段识别: 重叠率{overlap_rate:.1f}% >= 50%")
            criteria_met += 1
        else:
            print(f"  ❌ 积压时段识别: 重叠率{overlap_rate:.1f}% < 50%")
        
        if deviation <= 15:  # 最高峰偏差<=15%
            print(f"  ✅ 最高峰预测: 偏差{deviation:.1f}% <= 15%")
            criteria_met += 1
        else:
            print(f"  ❌ 最高峰预测: 偏差{deviation:.1f}% > 15%")
        
        accuracy_score = criteria_met / total_criteria * 100
        print(f"\n  仿真准确度: {accuracy_score:.0f}% ({criteria_met}/{total_criteria}项达标)")
        
        if accuracy_score >= 100:
            print(f"  🎯 仿真模型表现优秀，参数设置合理")
        elif accuracy_score >= 50:
            print(f"  ⚠️  仿真模型基本可用，建议进一步调优参数")
        else:
            print(f"  🔧 仿真模型需要重大调整")
            print(f"  建议调整方向:")
            if overlap_rate < 50:
                print(f"    - 调整ROT和尾流间隔参数以产生更多积压")
            if deviation > 15:
                if sim_avg_max < real_max_count:
                    print(f"    - 增加ROT时间和尾流间隔以增加积压程度")
                else:
                    print(f"    - 减少ROT时间和尾流间隔以减少积压程度")
        
        return {
            'overlap_rate': overlap_rate,
            'deviation': deviation,
            'accuracy_score': accuracy_score,
            'real_backlog_hours': real_backlog_hours,
            'sim_backlog_hours': all_sim_backlog_hours,
            'overlap_hours': overlap
        }
    
    def visualize_comparison(self, real_analysis, simulation_results):
        """可视化对比结果"""
        fig, axes = plt.subplots(2, 3, figsize=(20, 12))
        
        # 1. 真实与仿真延误分布对比 - 修正为日均对比
        ax1 = axes[0, 0]
        real_hourly = real_analysis['hourly_delays']  # 现在是日均数据
        
        # 汇总仿真数据
        sim_hourly_sum = pd.Series(0, index=range(24))
        for result in simulation_results.values():
            for hour, count in result['hourly_delays'].items():
                sim_hourly_sum[hour] += count
        sim_hourly_avg = sim_hourly_sum / len(simulation_results)
        
        x = range(24)
        width = 0.35
        ax1.bar([i - width/2 for i in x], [real_hourly.get(i, 0) for i in x], 
                width, label='真实数据(日均)', alpha=0.7, color='blue')
        ax1.bar([i + width/2 for i in x], [sim_hourly_avg.get(i, 0) for i in x], 
                width, label='仿真平均', alpha=0.7, color='red')
        ax1.axhline(y=self.backlog_threshold, color='orange', linestyle='--', alpha=0.7, label='积压阈值')
        ax1.set_xlabel('小时')
        ax1.set_ylabel('延误航班数')
        ax1.set_title('真实(日均)vs仿真延误分布对比')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 2. 积压时段重叠可视化
        ax2 = axes[0, 1]
        real_backlog = set(real_analysis['backlog_hours'])
        all_sim_backlog = set()
        for result in simulation_results.values():
            all_sim_backlog.update(result['backlog_hours'])
        
        all_hours = sorted(real_backlog.union(all_sim_backlog))
        if all_hours:
            real_mask = [h in real_backlog for h in all_hours]
            sim_mask = [h in all_sim_backlog for h in all_hours]
            
            y_pos = np.arange(len(all_hours))
            ax2.barh(y_pos - 0.2, real_mask, 0.4, label='真实积压', alpha=0.7)
            ax2.barh(y_pos + 0.2, sim_mask, 0.4, label='仿真积压', alpha=0.7)
            ax2.set_yticks(y_pos)
            ax2.set_yticklabels([f'{h:02d}:00' for h in all_hours])
            ax2.set_xlabel('是否积压')
            ax2.set_title('积压时段重叠分析')
            ax2.legend()
        
        # 3. 延误率对比
        ax3 = axes[0, 2]
        real_delay_rate = len(real_analysis['delayed_flights']) / len(self.real_data) * 100
        sim_delay_rates = [result['delay_rate'] for result in simulation_results.values()]
        
        categories = ['真实数据'] + [f'仿真{i+1}' for i in range(len(sim_delay_rates))]
        rates = [real_delay_rate] + sim_delay_rates
        colors = ['blue'] + ['red'] * len(sim_delay_rates)
        
        ax3.bar(categories, rates, alpha=0.7, color=colors)
        ax3.set_ylabel('延误率 (%)')
        ax3.set_title('延误率对比')
        ax3.tick_params(axis='x', rotation=45)
        
        # 4. 连续积压时段
        ax4 = axes[1, 0]
        continuous_periods = real_analysis['continuous_periods']
        if continuous_periods:
            for i, period in enumerate(continuous_periods):
                ax4.barh(i, len(period), left=period[0], alpha=0.7)
                ax4.text(period[0] + len(period)/2, i, f'{len(period)}h', 
                        ha='center', va='center')
            ax4.set_xlabel('小时')
            ax4.set_ylabel('连续积压时段')
            ax4.set_title('真实连续积压时段')
            ax4.set_xlim(0, 24)
        
        # 5. 参数影响分析
        ax5 = axes[1, 1]
        param_names = ['ROT增加', '尾流间隔增加', 'Taxi-out时间', '延误阈值降低']
        param_impacts = [25, 30, 20, 25]  # 示例影响程度
        
        ax5.pie(param_impacts, labels=param_names, autopct='%1.1f%%', startangle=90)
        ax5.set_title('参数调整对仿真的影响')
        
        # 6. 准确度评分
        ax6 = axes[1, 2]
        # 这里需要从compare_results的返回值中获取
        ax6.text(0.5, 0.5, '准确度评分\n待计算', ha='center', va='center', 
                fontsize=16, transform=ax6.transAxes)
        ax6.set_title('仿真模型评估')
        ax6.axis('off')
        
        plt.tight_layout()
        plt.savefig('ZGGG仿真现实对比分析.png', dpi=300, bbox_inches='tight')
        plt.show()
    
    def _find_continuous_periods(self, hours):
        """查找连续时段"""
        if not hours:
            return []
        
        hours = sorted(hours)
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

def main():
    """主函数"""
    print("=== ZGGG仿真与实际对比分析 ===")
    
    # 初始化对比分析器 - 使用优化后的参数
    comparator = SimulationRealityComparator(delay_threshold=4, backlog_threshold=10)
    
    # 1. 载入真实数据
    comparator.load_real_data()
    
    # 2. 分析真实积压模式
    real_analysis = comparator.analyze_real_backlog_patterns()
    
    # 3. 运行仿真分析
    simulation_results = comparator.run_simulation_analysis()
    
    # 4. 对比结果
    comparison = comparator.compare_results(real_analysis, simulation_results)
    
    # 5. 可视化对比
    comparator.visualize_comparison(real_analysis, simulation_results)
    
    return comparator, real_analysis, simulation_results, comparison

if __name__ == "__main__":
    comparator, real_analysis, simulation_results, comparison = main()
