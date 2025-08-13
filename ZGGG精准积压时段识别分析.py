#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG精准积压时段识别分析
按挑战杯规则进行精准的积压时段识别：
1. 积压时段判定：10个以上航班被积压即为积压时段
2. 不使用日均数据，对每天每个时段单独判定
3. 通过调整延误判定阈值来优化积压识别准确性
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

class PreciseBacklogAnalyzer:
    def __init__(self, delay_threshold=15, backlog_threshold=10):
        """
        精准积压分析器
        
        Args:
            delay_threshold: 延误判定阈值(分钟) - 可调整参数
            backlog_threshold: 积压判定阈值(班次/小时) - 固定为10按挑战杯规则
        """
        self.delay_threshold = delay_threshold
        self.backlog_threshold = backlog_threshold  # 固定为10
        self.real_data = None
        
        print(f"=== 精准积压分析器初始化 ===")
        print(f"延误判定阈值: {delay_threshold} 分钟")
        print(f"积压判定阈值: {backlog_threshold} 班/小时 (挑战杯标准)")
    
    def load_real_data(self):
        """载入真实数据"""
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
        
        # 添加时间维度字段
        self.real_data['date'] = self.real_data['计划离港时间'].dt.date
        self.real_data['hour'] = self.real_data['计划离港时间'].dt.hour
        
        print(f"真实数据载入: {len(self.real_data)} 班航班")
        print(f"数据时间范围: {self.real_data['date'].min()} 至 {self.real_data['date'].max()}")
        
        return self.real_data
    
    def analyze_daily_hourly_backlog(self):
        """按日按时分析积压情况"""
        print(f"\n=== 按日按时分析积压情况 ===")
        print(f"延误判定标准: >{self.delay_threshold}分钟")
        print(f"积压判定标准: >{self.backlog_threshold}班/小时")
        
        # 识别延误航班
        delayed_flights = self.real_data[
            self.real_data['起飞延误分钟'] > self.delay_threshold
        ].copy()
        
        print(f"延误航班总数: {len(delayed_flights)} 班 ({len(delayed_flights)/len(self.real_data)*100:.1f}%)")
        
        # 按日期和小时统计延误航班数
        daily_hourly_counts = delayed_flights.groupby(['date', 'hour']).size().reset_index(name='delayed_count')
        
        # 识别积压时段（每天每小时单独判定）
        backlog_periods = daily_hourly_counts[
            daily_hourly_counts['delayed_count'] > self.backlog_threshold
        ].copy()
        
        print(f"总积压时段数: {len(backlog_periods)} 个时段")
        
        # 按小时统计积压频次
        hourly_backlog_frequency = backlog_periods.groupby('hour').size().sort_index()
        
        print(f"\n各小时积压频次统计:")
        total_days = len(self.real_data['date'].unique())
        for hour in range(24):
            freq = hourly_backlog_frequency.get(hour, 0)
            percentage = freq / total_days * 100
            print(f"  {hour:02d}:00-{hour+1:02d}:00: {freq}/{total_days}天 ({percentage:.1f}%)")
        
        # 计算每小时的延误统计
        hourly_stats = {}
        for hour in range(24):
            hour_data = daily_hourly_counts[daily_hourly_counts['hour'] == hour]['delayed_count']
            if len(hour_data) > 0:
                hourly_stats[hour] = {
                    'mean': hour_data.mean(),
                    'std': hour_data.std(),
                    'max': hour_data.max(),
                    'min': hour_data.min(),
                    'backlog_days': hourly_backlog_frequency.get(hour, 0),
                    'backlog_rate': hourly_backlog_frequency.get(hour, 0) / total_days * 100
                }
            else:
                hourly_stats[hour] = {
                    'mean': 0, 'std': 0, 'max': 0, 'min': 0,
                    'backlog_days': 0, 'backlog_rate': 0
                }
        
        return {
            'delayed_flights': delayed_flights,
            'daily_hourly_counts': daily_hourly_counts,
            'backlog_periods': backlog_periods,
            'hourly_backlog_frequency': hourly_backlog_frequency,
            'hourly_stats': hourly_stats,
            'total_days': total_days
        }
    
    def identify_frequent_backlog_hours(self, analysis_result, min_frequency_rate=20):
        """识别经常出现积压的时段"""
        print(f"\n=== 识别频繁积压时段 ===")
        print(f"频繁积压判定标准: 积压发生率 >= {min_frequency_rate}%")
        
        hourly_stats = analysis_result['hourly_stats']
        frequent_backlog_hours = []
        
        for hour in range(24):
            stats = hourly_stats[hour]
            if stats['backlog_rate'] >= min_frequency_rate:
                frequent_backlog_hours.append(hour)
                print(f"  {hour:02d}:00-{hour+1:02d}:00: 积压率{stats['backlog_rate']:.1f}% "
                      f"({stats['backlog_days']}/{analysis_result['total_days']}天)")
        
        print(f"频繁积压时段: {len(frequent_backlog_hours)} 个: {frequent_backlog_hours}")
        
        return frequent_backlog_hours
    
    def analyze_backlog_patterns(self, analysis_result):
        """分析积压模式"""
        print(f"\n=== 积压模式分析 ===")
        
        backlog_periods = analysis_result['backlog_periods']
        
        # 按日期分析积压情况
        daily_backlog_counts = backlog_periods.groupby('date').size()
        print(f"每日积压时段数统计:")
        print(f"  平均每日积压时段: {daily_backlog_counts.mean():.1f} 个")
        print(f"  最多积压时段: {daily_backlog_counts.max()} 个")
        print(f"  最少积压时段: {daily_backlog_counts.min()} 个")
        print(f"  无积压天数: {analysis_result['total_days'] - len(daily_backlog_counts)} 天")
        
        # 找出积压最严重的日期
        worst_days = daily_backlog_counts.nlargest(5)
        print(f"\n积压最严重的5天:")
        for date, count in worst_days.items():
            day_details = backlog_periods[backlog_periods['date'] == date]
            hours = sorted(day_details['hour'].tolist())
            delayed_total = day_details['delayed_count'].sum()
            print(f"  {date}: {count}个积压时段, 时段{hours}, 总延误{delayed_total}班")
        
        # 分析连续积压情况
        continuous_backlog_analysis = self.analyze_continuous_backlog_by_day(backlog_periods)
        
        return {
            'daily_backlog_counts': daily_backlog_counts,
            'worst_days': worst_days,
            'continuous_analysis': continuous_backlog_analysis
        }
    
    def analyze_continuous_backlog_by_day(self, backlog_periods):
        """分析每天的连续积压情况"""
        print(f"\n=== 连续积压分析 ===")
        
        continuous_stats = []
        
        for date in backlog_periods['date'].unique():
            day_backlog = backlog_periods[backlog_periods['date'] == date]
            hours = sorted(day_backlog['hour'].tolist())
            
            # 找连续时段
            continuous_periods = []
            if hours:
                current_period = [hours[0]]
                for i in range(1, len(hours)):
                    if hours[i] - hours[i-1] == 1:
                        current_period.append(hours[i])
                    else:
                        continuous_periods.append(current_period)
                        current_period = [hours[i]]
                continuous_periods.append(current_period)
            
            # 统计连续时段信息
            for period in continuous_periods:
                if len(period) >= 1:  # 至少1小时的积压
                    duration = len(period)
                    start_hour = period[0]
                    end_hour = period[-1]
                    total_delayed = day_backlog[day_backlog['hour'].isin(period)]['delayed_count'].sum()
                    
                    continuous_stats.append({
                        'date': date,
                        'start_hour': start_hour,
                        'end_hour': end_hour,
                        'duration': duration,
                        'total_delayed': total_delayed,
                        'period': period
                    })
        
        # 分析连续积压统计
        if continuous_stats:
            durations = [s['duration'] for s in continuous_stats]
            print(f"连续积压时段统计:")
            print(f"  总连续积压时段数: {len(continuous_stats)} 个")
            print(f"  平均持续时长: {np.mean(durations):.1f} 小时")
            print(f"  最长持续时长: {max(durations)} 小时")
            print(f"  最短持续时长: {min(durations)} 小时")
            
            # 按持续时长分类
            duration_counts = pd.Series(durations).value_counts().sort_index()
            print(f"  持续时长分布:")
            for duration, count in duration_counts.items():
                print(f"    {duration}小时: {count} 次")
            
            # 找出最长的几个连续积压
            longest_periods = sorted(continuous_stats, key=lambda x: x['duration'], reverse=True)[:5]
            print(f"\n最长连续积压时段TOP5:")
            for i, period in enumerate(longest_periods, 1):
                print(f"  {i}. {period['date']} {period['start_hour']:02d}:00-{period['end_hour']+1:02d}:00 "
                      f"(持续{period['duration']}小时, {period['total_delayed']}班延误)")
        
        return {
            'continuous_stats': continuous_stats,
            'duration_distribution': pd.Series(durations).value_counts().sort_index() if continuous_stats else pd.Series()
        }
    
    def test_different_delay_thresholds(self, thresholds=[5, 10, 15, 20, 25, 30]):
        """测试不同延误阈值对积压识别的影响"""
        print(f"\n=== 测试不同延误阈值影响 ===")
        
        threshold_results = {}
        
        for threshold in thresholds:
            print(f"\n测试延误阈值: {threshold}分钟")
            
            # 临时修改阈值
            original_threshold = self.delay_threshold
            self.delay_threshold = threshold
            
            # 分析结果
            analysis_result = self.analyze_daily_hourly_backlog()
            frequent_hours = self.identify_frequent_backlog_hours(analysis_result, min_frequency_rate=20)
            
            # 统计关键指标
            total_backlog_periods = len(analysis_result['backlog_periods'])
            total_delayed = len(analysis_result['delayed_flights'])
            delay_rate = total_delayed / len(self.real_data) * 100
            
            threshold_results[threshold] = {
                'total_backlog_periods': total_backlog_periods,
                'total_delayed_flights': total_delayed,
                'delay_rate': delay_rate,
                'frequent_backlog_hours': len(frequent_hours),
                'hourly_backlog_frequency': analysis_result['hourly_backlog_frequency']
            }
            
            print(f"  延误航班: {total_delayed} 班 ({delay_rate:.1f}%)")
            print(f"  积压时段: {total_backlog_periods} 个")
            print(f"  频繁积压小时: {len(frequent_hours)} 个")
            
            # 恢复原始阈值
            self.delay_threshold = original_threshold
        
        return threshold_results
    
    def recommend_optimal_threshold(self, threshold_results):
        """推荐最优延误阈值"""
        print(f"\n=== 推荐最优延误阈值 ===")
        
        print(f"不同阈值对比:")
        print(f"{'阈值(min)':<8} {'延误率(%)':<10} {'积压时段':<8} {'频繁积压小时':<12}")
        print("-" * 50)
        
        for threshold, result in threshold_results.items():
            print(f"{threshold:<8} {result['delay_rate']:<10.1f} {result['total_backlog_periods']:<8} {result['frequent_backlog_hours']:<12}")
        
        # 寻找合适的阈值：积压时段数量适中（不要太多也不要太少）
        # 目标：频繁积压小时数在3-8个之间，总积压时段数合理
        optimal_candidates = []
        for threshold, result in threshold_results.items():
            frequent_hours = result['frequent_backlog_hours']
            total_periods = result['total_backlog_periods']
            
            # 评分标准
            score = 0
            if 3 <= frequent_hours <= 8:  # 频繁积压时段数合理
                score += 30
            if 50 <= total_periods <= 200:  # 总积压时段数合理
                score += 30
            if 30 <= result['delay_rate'] <= 70:  # 延误率合理
                score += 20
            
            # 额外奖励：接近理想值的情况
            if frequent_hours == 5:  # 理想的频繁积压时段数
                score += 10
            if 80 <= total_periods <= 120:  # 理想的总积压时段数
                score += 10
            
            optimal_candidates.append((threshold, score, result))
        
        # 找出最优阈值
        optimal_candidates.sort(key=lambda x: x[1], reverse=True)
        optimal_threshold, optimal_score, optimal_result = optimal_candidates[0]
        
        print(f"\n推荐最优延误阈值: {optimal_threshold} 分钟")
        print(f"评分: {optimal_score}/100")
        print(f"该阈值下:")
        print(f"  延误率: {optimal_result['delay_rate']:.1f}%")
        print(f"  总积压时段: {optimal_result['total_backlog_periods']} 个")
        print(f"  频繁积压时段: {optimal_result['frequent_backlog_hours']} 个小时")
        
        return optimal_threshold, optimal_result

def main():
    """主函数"""
    print("=== ZGGG精准积压时段识别分析 ===")
    
    # 初始化分析器
    analyzer = PreciseBacklogAnalyzer(delay_threshold=15, backlog_threshold=10)
    
    # 载入数据
    analyzer.load_real_data()
    
    # 测试不同延误阈值
    threshold_results = analyzer.test_different_delay_thresholds()
    
    # 推荐最优阈值
    optimal_threshold, optimal_result = analyzer.recommend_optimal_threshold(threshold_results)
    
    # 使用最优阈值进行详细分析
    print(f"\n" + "="*60)
    print(f"            使用最优阈值 ({optimal_threshold}分钟) 进行详细分析")
    print(f"="*60)
    
    analyzer.delay_threshold = optimal_threshold
    final_analysis = analyzer.analyze_daily_hourly_backlog()
    frequent_hours = analyzer.identify_frequent_backlog_hours(final_analysis)
    pattern_analysis = analyzer.analyze_backlog_patterns(final_analysis)
    
    # 输出最终结果
    print(f"\n" + "="*60)
    print(f"                最终分析结果")
    print(f"="*60)
    print(f"✅ 推荐延误判定阈值: {optimal_threshold} 分钟")
    print(f"✅ 积压判定阈值: {analyzer.backlog_threshold} 班/小时 (挑战杯标准)")
    print(f"✅ 总延误航班: {len(final_analysis['delayed_flights'])} 班")
    print(f"✅ 总积压时段: {len(final_analysis['backlog_periods'])} 个")
    print(f"✅ 频繁积压时段: {len(frequent_hours)} 个: {frequent_hours}")
    print(f"✅ 平均每日积压时段: {pattern_analysis['daily_backlog_counts'].mean():.1f} 个")
    
    return analyzer, final_analysis, pattern_analysis, threshold_results

if __name__ == "__main__":
    analyzer, final_analysis, pattern_analysis, threshold_results = main()
