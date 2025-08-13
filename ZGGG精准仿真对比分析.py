#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG精准仿真对比分析
基于优化的积压识别方法进行仿真与真实数据对比
1. 使用30分钟延误阈值，10班积压阈值（挑战杯标准）
2. 逐日逐时段分析，不使用日均数据
3. 精准识别真实积压时段进行对比验证
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

class PreciseSimulationComparator:
    def __init__(self, delay_threshold=30, backlog_threshold=10, taxi_out_time=10):
        """
        精准仿真对比分析器
        
        Args:
            delay_threshold: 延误判定阈值(分钟) - 优化后参数
            backlog_threshold: 积压判定阈值(班次/小时) - 挑战杯标准
            taxi_out_time: 滑行出港时间(分钟)
        """
        self.delay_threshold = delay_threshold
        self.backlog_threshold = backlog_threshold
        self.taxi_out_time = taxi_out_time
        self.real_data = None
        self.simulation_results = {}
        
        print(f"=== 精准仿真对比分析器初始化 ===")
        print(f"延误判定阈值: {delay_threshold} 分钟 (优化后)")
        print(f"积压判定阈值: {backlog_threshold} 班/小时 (挑战杯标准)")
        print(f"滑行出港时间: {taxi_out_time} 分钟")
    
    def load_and_analyze_real_data(self):
        """载入并分析真实数据"""
        print(f"\n=== 载入并分析真实数据 ===")
        
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
        
        # 添加时间维度字段
        self.real_data['date'] = self.real_data['计划离港时间'].dt.date
        self.real_data['hour'] = self.real_data['计划离港时间'].dt.hour
        
        print(f"真实数据载入: {len(self.real_data)} 班航班")
        
        # 分析真实积压情况
        return self.analyze_real_backlog_daily()
    
    def analyze_real_backlog_daily(self):
        """按日按时分析真实积压情况"""
        print(f"\n=== 分析真实积压情况 ===")
        
        # 识别延误航班
        delayed_flights = self.real_data[
            self.real_data['起飞延误分钟'] > self.delay_threshold
        ].copy()
        
        print(f"延误航班数: {len(delayed_flights)} 班 ({len(delayed_flights)/len(self.real_data)*100:.1f}%)")
        
        # 按日期和小时统计延误航班数
        daily_hourly_counts = delayed_flights.groupby(['date', 'hour']).size().reset_index(name='delayed_count')
        
        # 识别积压时段（每天每小时单独判定）
        real_backlog_periods = daily_hourly_counts[
            daily_hourly_counts['delayed_count'] > self.backlog_threshold
        ].copy()
        
        print(f"真实积压时段数: {len(real_backlog_periods)} 个")
        
        # 按小时统计积压频次
        hourly_backlog_frequency = real_backlog_periods.groupby('hour').size()
        frequent_backlog_hours = hourly_backlog_frequency[hourly_backlog_frequency >= 6].index.tolist()  # 至少6天出现积压
        
        print(f"频繁积压时段: {len(frequent_backlog_hours)} 个: {sorted(frequent_backlog_hours)}")
        
        return {
            'delayed_flights': delayed_flights,
            'daily_hourly_counts': daily_hourly_counts,
            'backlog_periods': real_backlog_periods,
            'frequent_backlog_hours': sorted(frequent_backlog_hours),
            'hourly_backlog_frequency': hourly_backlog_frequency
        }
    
    def run_simulation_analysis(self, test_dates=None):
        """运行仿真分析"""
        print(f"\n=== 运行仿真分析 ===")
        
        # 初始化仿真器
        simulator = ZGGGDepartureSimulator(
            delay_threshold=self.delay_threshold,
            backlog_threshold=self.backlog_threshold,
            taxi_out_time=self.taxi_out_time
        )
        
        # 载入数据
        simulator.load_departure_data()
        simulator.identify_weather_suspended_periods()
        simulator.classify_aircraft_types()
        simulator.separate_flight_types()
        
        # 选择测试日期 - 包括不同类型的日期
        if test_dates is None:
            daily_counts = simulator.data['计划离港时间'].dt.date.value_counts()
            sorted_dates = daily_counts.sort_values(ascending=False)
            test_dates = [
                sorted_dates.index[0],   # 最繁忙日期
                sorted_dates.index[5],   # 次繁忙日期
                sorted_dates.index[15],  # 中等日期
                sorted_dates.index[25],  # 相对轻松日期
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
    
    def compare_precise_results(self, real_analysis, simulation_results):
        """精准对比仿真与现实结果"""
        print(f"\n" + "="*70)
        print(f"                精准仿真与现实对比分析")
        print(f"="*70)
        
        # 1. 频繁积压时段对比
        real_frequent_hours = set(real_analysis['frequent_backlog_hours'])
        
        # 汇总仿真的积压时段
        all_sim_backlog_hours = set()
        for date, result in simulation_results.items():
            all_sim_backlog_hours.update(result['backlog_hours'])
        
        # 计算重叠度
        overlap = real_frequent_hours.intersection(all_sim_backlog_hours)
        overlap_rate = len(overlap) / len(real_frequent_hours) * 100 if len(real_frequent_hours) > 0 else 0
        
        print(f"\n【频繁积压时段对比】")
        print(f"  真实频繁积压时段: {len(real_frequent_hours)} 个: {sorted(real_frequent_hours)}")
        print(f"  仿真积压时段: {len(all_sim_backlog_hours)} 个: {sorted(all_sim_backlog_hours)}")
        print(f"  重叠时段: {len(overlap)} 个: {sorted(overlap)}")
        print(f"  重叠率: {overlap_rate:.1f}%")
        
        # 2. 积压强度对比
        # 真实数据中频繁积压时段的平均强度
        real_frequent_intensity = {}
        for hour in real_frequent_hours:
            freq = real_analysis['hourly_backlog_frequency'].get(hour, 0)
            real_frequent_intensity[hour] = freq
        
        # 仿真数据中的积压强度
        sim_frequent_intensity = {}
        for hour in all_sim_backlog_hours:
            count = 0
            for result in simulation_results.values():
                if hour in result['backlog_hours']:
                    count += 1
            sim_frequent_intensity[hour] = count
        
        print(f"\n【积压强度对比】")
        print(f"  真实频繁积压时段强度 (出现天数):")
        for hour in sorted(real_frequent_intensity.keys()):
            print(f"    {hour:02d}:00-{hour+1:02d}:00: {real_frequent_intensity[hour]} 天")
        
        print(f"  仿真积压时段强度 (出现次数):")
        for hour in sorted(sim_frequent_intensity.keys()):
            print(f"    {hour:02d}:00-{hour+1:02d}:00: {sim_frequent_intensity[hour]} 次")
        
        # 3. 延误率对比
        real_delay_rate = len(real_analysis['delayed_flights']) / len(self.real_data) * 100
        sim_delay_rates = [result['delay_rate'] for result in simulation_results.values()]
        sim_avg_delay_rate = np.mean(sim_delay_rates) if sim_delay_rates else 0
        
        delay_rate_deviation = abs(sim_avg_delay_rate - real_delay_rate) / real_delay_rate * 100 if real_delay_rate > 0 else 100
        
        print(f"\n【延误率对比】")
        print(f"  真实延误率: {real_delay_rate:.1f}%")
        print(f"  仿真平均延误率: {sim_avg_delay_rate:.1f}%")
        print(f"  延误率偏差: {delay_rate_deviation:.1f}%")
        
        # 4. 总体评估
        print(f"\n【仿真模型评估】")
        criteria_met = 0
        total_criteria = 3
        
        # 评估标准1：积压时段重叠率>=60%
        if overlap_rate >= 60:
            print(f"  ✅ 积压时段识别: 重叠率{overlap_rate:.1f}% >= 60%")
            criteria_met += 1
        else:
            print(f"  ❌ 积压时段识别: 重叠率{overlap_rate:.1f}% < 60%")
        
        # 评估标准2：延误率偏差<=30%
        if delay_rate_deviation <= 30:
            print(f"  ✅ 延误率预测: 偏差{delay_rate_deviation:.1f}% <= 30%")
            criteria_met += 1
        else:
            print(f"  ❌ 延误率预测: 偏差{delay_rate_deviation:.1f}% > 30%")
        
        # 评估标准3：识别出的积压时段数量合理(不过多也不过少)
        reasonable_backlog_count = 3 <= len(all_sim_backlog_hours) <= 10
        if reasonable_backlog_count:
            print(f"  ✅ 积压时段数量: {len(all_sim_backlog_hours)} 个 (合理范围3-10个)")
            criteria_met += 1
        else:
            print(f"  ❌ 积压时段数量: {len(all_sim_backlog_hours)} 个 (不在合理范围3-10个)")
        
        accuracy_score = criteria_met / total_criteria * 100
        print(f"\n  仿真准确度: {accuracy_score:.0f}% ({criteria_met}/{total_criteria}项达标)")
        
        if accuracy_score >= 100:
            print(f"  🎯 仿真模型表现优秀，参数设置合理")
        elif accuracy_score >= 67:
            print(f"  ✅ 仿真模型表现良好，基本可用")
        elif accuracy_score >= 33:
            print(f"  ⚠️  仿真模型基本可用，建议进一步调优")
        else:
            print(f"  🔧 仿真模型需要重大调整")
        
        return {
            'overlap_rate': overlap_rate,
            'delay_rate_deviation': delay_rate_deviation,
            'accuracy_score': accuracy_score,
            'real_frequent_hours': real_frequent_hours,
            'sim_backlog_hours': all_sim_backlog_hours,
            'overlap_hours': overlap
        }
    
    def visualize_precise_comparison(self, real_analysis, simulation_results):
        """可视化精准对比结果"""
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        
        # 1. 频繁积压时段对比
        ax1 = axes[0, 0]
        real_hours = real_analysis['frequent_backlog_hours']
        real_frequencies = [real_analysis['hourly_backlog_frequency'].get(h, 0) for h in real_hours]
        
        if real_hours:
            ax1.bar(range(len(real_hours)), real_frequencies, alpha=0.7, color='blue', label='真实频次')
            ax1.set_xticks(range(len(real_hours)))
            ax1.set_xticklabels([f'{h:02d}h' for h in real_hours])
            ax1.set_ylabel('积压天数')
            ax1.set_title('真实频繁积压时段')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
        
        # 2. 仿真积压时段分布
        ax2 = axes[0, 1]
        all_sim_hours = set()
        for result in simulation_results.values():
            all_sim_hours.update(result['backlog_hours'])
        
        if all_sim_hours:
            sim_hours = sorted(all_sim_hours)
            sim_frequencies = []
            for hour in sim_hours:
                count = sum(1 for result in simulation_results.values() if hour in result['backlog_hours'])
                sim_frequencies.append(count)
            
            ax2.bar(range(len(sim_hours)), sim_frequencies, alpha=0.7, color='red', label='仿真频次')
            ax2.set_xticks(range(len(sim_hours)))
            ax2.set_xticklabels([f'{h:02d}h' for h in sim_hours])
            ax2.set_ylabel('积压次数')
            ax2.set_title('仿真积压时段分布')
            ax2.legend()
            ax2.grid(True, alpha=0.3)
        
        # 3. 重叠时段可视化
        ax3 = axes[0, 2]
        real_frequent_hours = set(real_analysis['frequent_backlog_hours'])
        all_sim_backlog_hours = set()
        for result in simulation_results.values():
            all_sim_backlog_hours.update(result['backlog_hours'])
        
        all_hours = sorted(real_frequent_hours.union(all_sim_backlog_hours))
        if all_hours:
            real_mask = [1 if h in real_frequent_hours else 0 for h in all_hours]
            sim_mask = [1 if h in all_sim_backlog_hours else 0 for h in all_hours]
            overlap_mask = [1 if h in real_frequent_hours and h in all_sim_backlog_hours else 0 for h in all_hours]
            
            width = 0.25
            x = np.arange(len(all_hours))
            ax3.bar(x - width, real_mask, width, label='真实', alpha=0.7, color='blue')
            ax3.bar(x, sim_mask, width, label='仿真', alpha=0.7, color='red')
            ax3.bar(x + width, overlap_mask, width, label='重叠', alpha=0.7, color='green')
            
            ax3.set_xticks(x)
            ax3.set_xticklabels([f'{h:02d}h' for h in all_hours])
            ax3.set_ylabel('是否积压')
            ax3.set_title('积压时段重叠分析')
            ax3.legend()
            ax3.grid(True, alpha=0.3)
        
        # 4. 延误率对比
        ax4 = axes[1, 0]
        real_delay_rate = len(real_analysis['delayed_flights']) / len(self.real_data) * 100
        sim_delay_rates = [result['delay_rate'] for result in simulation_results.values()]
        
        categories = ['真实数据'] + [f'仿真{i+1}' for i in range(len(sim_delay_rates))]
        rates = [real_delay_rate] + sim_delay_rates
        colors = ['blue'] + ['red'] * len(sim_delay_rates)
        
        bars = ax4.bar(categories, rates, alpha=0.7, color=colors)
        ax4.set_ylabel('延误率 (%)')
        ax4.set_title('延误率对比')
        ax4.tick_params(axis='x', rotation=45)
        ax4.grid(True, alpha=0.3)
        
        # 添加数值标签
        for bar, rate in zip(bars, rates):
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{rate:.1f}%', ha='center', va='bottom')
        
        # 5. 积压时段数量对比
        ax5 = axes[1, 1]
        real_total_backlog = len(real_analysis['backlog_periods'])
        sim_total_backlogs = [len([h for result in simulation_results.values() 
                                 for h in result['backlog_hours']]) for _ in range(1)]
        
        comparison_data = {
            '真实总积压时段': real_total_backlog,
            '真实频繁积压': len(real_analysis['frequent_backlog_hours']),
            '仿真积压时段': len(all_sim_backlog_hours)
        }
        
        bars = ax5.bar(comparison_data.keys(), comparison_data.values(), 
                       color=['blue', 'lightblue', 'red'], alpha=0.7)
        ax5.set_ylabel('积压时段数量')
        ax5.set_title('积压时段数量对比')
        plt.setp(ax5.get_xticklabels(), rotation=45, ha='right')
        
        # 添加数值标签
        for bar, (key, value) in zip(bars, comparison_data.items()):
            height = bar.get_height()
            ax5.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{value}', ha='center', va='bottom')
        
        # 6. 评估雷达图
        ax6 = axes[1, 2]
        # 计算各项评分
        comparison_result = self.compare_precise_results(real_analysis, simulation_results)
        
        categories = ['积压时段\n识别', '延误率\n预测', '时段数量\n合理性', '总体\n准确性']
        overlap_score = min(100, comparison_result['overlap_rate'] * 100/60)  # 标准化到100分
        delay_score = max(0, 100 - comparison_result['delay_rate_deviation'] * 100/30)  # 标准化到100分
        quantity_score = 100 if 3 <= len(all_sim_backlog_hours) <= 10 else 50
        overall_score = comparison_result['accuracy_score']
        
        values = [overlap_score, delay_score, quantity_score, overall_score]
        
        # 雷达图
        angles = np.linspace(0, 2 * np.pi, len(categories), endpoint=False)
        values_closed = np.concatenate((values, [values[0]]))  # 闭合
        angles_closed = np.concatenate((angles, [angles[0]]))  # 闭合
        
        ax6.plot(angles_closed, values_closed, 'o-', linewidth=2, color='green')
        ax6.fill(angles_closed, values_closed, alpha=0.25, color='green')
        ax6.set_xticks(angles)
        ax6.set_xticklabels(categories)
        ax6.set_ylim(0, 100)
        ax6.set_yticks([20, 40, 60, 80, 100])
        ax6.set_yticklabels(['20', '40', '60', '80', '100'])
        ax6.set_title('仿真模型综合评估')
        ax6.grid(True)
        
        plt.tight_layout()
        plt.savefig('ZGGG精准仿真对比分析.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        return fig

def main():
    """主函数"""
    print("=== ZGGG精准仿真对比分析 ===")
    
    # 初始化分析器 - 使用优化后的参数
    comparator = PreciseSimulationComparator(
        delay_threshold=30,  # 优化后的延误阈值
        backlog_threshold=10, # 挑战杯标准
        taxi_out_time=10
    )
    
    # 1. 载入并分析真实数据
    real_analysis = comparator.load_and_analyze_real_data()
    
    # 2. 运行仿真分析
    simulation_results = comparator.run_simulation_analysis()
    
    # 3. 精准对比结果
    comparison = comparator.compare_precise_results(real_analysis, simulation_results)
    
    # 4. 可视化对比
    comparator.visualize_precise_comparison(real_analysis, simulation_results)
    
    # 5. 输出总结
    print(f"\n" + "="*70)
    print(f"                    分析总结")
    print(f"="*70)
    print(f"🎯 使用优化参数:")
    print(f"   • 延误判定阈值: 30分钟 (通过测试优化)")
    print(f"   • 积压判定阈值: 10班/小时 (挑战杯标准)")
    print(f"   • 分析方法: 逐日逐时段精准判定")
    print(f"\n📊 关键结果:")
    print(f"   • 真实频繁积压时段: {len(real_analysis['frequent_backlog_hours'])} 个")
    print(f"   • 仿真识别积压时段: {len(comparison['sim_backlog_hours'])} 个") 
    print(f"   • 时段重叠率: {comparison['overlap_rate']:.1f}%")
    print(f"   • 延误率偏差: {comparison['delay_rate_deviation']:.1f}%")
    print(f"   • 仿真准确度: {comparison['accuracy_score']:.0f}%")
    
    return comparator, real_analysis, simulation_results, comparison

if __name__ == "__main__":
    comparator, real_analysis, simulation_results, comparison = main()
