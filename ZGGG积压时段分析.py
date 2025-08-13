#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG机场积压时段分析
专门分析真实航班数据，探索合理的延误判定标准
避免全天都是积压时段的现象
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

class ZGGGBacklogAnalyzer:
    def __init__(self):
        """初始化积压分析器"""
        self.data = None
        self.backlog_threshold = 10  # 积压判定阈值：延误航班数>=10
        
        print("=== ZGGG机场积压时段分析器 ===")
        print(f"积压判定标准: 延误航班数 >= {self.backlog_threshold} 班/小时")
        print("新增功能: 系统性问题时段识别与过滤")
    
    def identify_weather_suspension_periods(self, data):
        """识别天气停飞时段"""
        print(f"\n=== 识别天气停飞时段 ===")
        
        # 按日期分组，寻找停飞时段
        weather_periods = []
        
        for date in data['计划离港时间'].dt.date.unique():
            day_data = data[data['计划离港时间'].dt.date == date].copy()
            day_data = day_data.sort_values('计划离港时间')
            
            # 寻找连续的长时间延误或停飞
            # 改进标准：需要足够的航班样本才能判定为系统性停飞
            for hour in range(24):
                hour_data = day_data[day_data['计划离港时间'].dt.hour == hour]
                if len(hour_data) == 0:
                    continue
                
                # 检查该小时是否有异常的长延误
                if '实际起飞时间' in hour_data.columns:
                    delays = (
                        hour_data['实际起飞时间'] - hour_data['计划离港时间']
                    ).dt.total_seconds() / 60
                    
                    # 改进的天气停飞判定条件
                    severe_delays = (delays > 120).sum()
                    
                    # 新的条件：至少需要3班航班，且80%以上严重延误才认为是系统性停飞
                    if (len(hour_data) >= 3 and  # 至少3班航班
                        severe_delays / len(hour_data) > 0.8):  # 80%以上严重延误
                        
                        # 寻找停飞时段的开始和结束
                        suspend_start = hour_data['计划离港时间'].min()
                        
                        # 估计停飞结束时间：找到延误恢复正常的时间点
                        normal_delay_threshold = 60  # 延误小于60分钟认为恢复正常
                        
                        # 向后查找，直到找到延误恢复正常的时间点
                        suspend_end = None
                        for check_hour in range(hour, 24):
                            check_data = day_data[day_data['计划离港时间'].dt.hour == check_hour]
                            if len(check_data) > 0:
                                check_delays = (
                                    check_data['实际起飞时间'] - check_data['计划离港时间']
                                ).dt.total_seconds() / 60
                                normal_flights = (check_delays <= normal_delay_threshold).sum()
                                
                                if len(check_data) > 0 and normal_flights / len(check_data) > 0.5:
                                    suspend_end = check_data['实际起飞时间'].min()
                                    break
                        
                        if suspend_end is None:
                            # 如果找不到恢复时间，使用当天最后一班延误航班的起飞时间
                            suspend_end = hour_data['实际起飞时间'].max()
                        
                        weather_periods.append({
                            'date': date,
                            'suspend_start': suspend_start,
                            'suspend_end': suspend_end,
                            'affected_flights': len(hour_data)
                        })
                        
                        print(f"发现天气停飞: {date} {suspend_start.strftime('%H:%M')}-{suspend_end.strftime('%H:%M')} 影响{len(hour_data)}班")
                    elif severe_delays > 0:
                        # 记录被跳过的潜在个别延误情况
                        print(f"跳过个别延误: {date} {hour:02d}:00时段 - 航班数{len(hour_data)}班，严重延误{severe_delays}班 (可能是个别情况)")
        
        print(f"识别到 {len(weather_periods)} 个天气停飞时段")
        return weather_periods
    
    def identify_exceptional_delays(self, data):
        """识别并标记特殊延误航班（个别情况），这些航班不应计入积压分析"""
        print(f"\n=== 识别特殊延误航班 ===")
        
        exceptional_flights = []
        
        # 按日期和小时分组分析
        for date in data['计划离港时间'].dt.date.unique():
            day_data = data[data['计划离港时间'].dt.date == date].copy()
            
            for hour in range(24):
                hour_data = day_data[day_data['计划离港时间'].dt.hour == hour]
                if len(hour_data) <= 1:  # 只有1班或更少，跳过
                    continue
                
                if '实际起飞时间' in hour_data.columns:
                    delays = (
                        hour_data['实际起飞时间'] - hour_data['计划离港时间']
                    ).dt.total_seconds() / 60
                    
                    # 计算该时段的延误分布
                    normal_delays = (delays <= 60).sum()  # 正常延误（<=60分钟）
                    severe_delays = (delays > 120).sum()  # 严重延误（>120分钟）
                    
                    # 如果只有少数航班严重延误，而大多数正常，则认为是个别情况
                    if (len(hour_data) >= 3 and  # 至少3班航班
                        severe_delays <= 2 and  # 严重延误不超过2班
                        normal_delays / len(hour_data) > 0.6):  # 60%以上航班延误正常
                        
                        # 找出严重延误的航班
                        severe_delay_flights = hour_data[delays > 120]
                        for idx in severe_delay_flights.index:
                            flight_delay = delays[idx]
                            exceptional_flights.append({
                                'index': idx,
                                'date': date,
                                'hour': hour,
                                'delay_minutes': flight_delay,
                                'reason': '个别严重延误'
                            })
                            print(f"标记特殊延误: {date} {hour:02d}:00时段 - 延误{flight_delay:.0f}分钟 (个别情况)")
        
        print(f"识别到 {len(exceptional_flights)} 个特殊延误航班，将从积压分析中排除")
        return exceptional_flights
    
    def identify_systematic_problematic_hours(self, data):
        """识别系统性问题时段（整个小时段都有异常延误）"""
        print(f"\n=== 识别系统性问题时段 ===")
        
        problematic_hours = []
        
        # 分析每个小时的整体延误情况
        for hour in range(24):
            hour_data = data[data['计划离港时间'].dt.hour == hour]
            if len(hour_data) < 5:  # 样本太少，跳过（降低最小样本要求）
                continue
                
            if '起飞延误分钟' in hour_data.columns:
                delays = hour_data['起飞延误分钟']
                
                avg_delay = delays.mean()
                severe_delay_ratio = (delays > 120).sum() / len(delays) if len(delays) > 0 else 0
                
                # 系统性问题的判定条件（针对不同时段采用不同标准）：
                is_problematic = False
                
                if 0 <= hour <= 6:  # 凌晨时段（0-6点）更严格的异常判定
                    # 凌晨时段航班少，但如果平均延误超过100分钟就不正常
                    if ((avg_delay > 100 and severe_delay_ratio > 0.2) or  # 平均延误>100分钟且20%严重延误
                        (avg_delay > 200) or  # 或平均延误>200分钟
                        (severe_delay_ratio > 0.4)):  # 或严重延误比例>40%
                        is_problematic = True
                        
                else:  # 其他时段（7-23点）的判定标准
                    if (avg_delay > 200 and 
                        severe_delay_ratio > 0.5 and 
                        len(hour_data) >= 10):
                        is_problematic = True
                
                if is_problematic:
                    problematic_hours.append({
                        'hour': hour,
                        'avg_delay': avg_delay,
                        'severe_ratio': severe_delay_ratio,
                        'total_flights': len(hour_data)
                    })
                    
                    print(f"识别系统性问题时段: {hour:02d}:00 - 平均延误{avg_delay:.0f}分钟, "
                          f"严重延误比例{severe_delay_ratio:.1%}, 总航班{len(hour_data)}班")
        
        return problematic_hours
    
    def identify_congestion_periods_advanced(self, threshold=15):
        """高级积压时段识别，排除天气停飞、特殊延误和系统性问题时段"""
        print(f"\n=== 高级积压时段分析（排除特殊情况）===")
        
        data = self.data.copy()
        
        # 首先识别天气停飞时段
        weather_periods = self.identify_weather_suspension_periods(data)
        
        # 识别系统性问题时段
        problematic_hours = self.identify_systematic_problematic_hours(data)
        
        # 排除系统性问题时段的数据
        filtered_data = data.copy()
        if problematic_hours:
            problematic_hour_list = [h['hour'] for h in problematic_hours]
            original_count = len(filtered_data)
            filtered_data = filtered_data[~filtered_data['计划离港时间'].dt.hour.isin(problematic_hour_list)]
            excluded_count = original_count - len(filtered_data)
            print(f"排除系统性问题时段数据: {excluded_count} 个航班")
        
        # 识别特殊延误航班（在剩余数据中）
        exceptional_flights = self.identify_exceptional_delays(filtered_data)
        exceptional_indices = {flight['index'] for flight in exceptional_flights}
        
        # 排除特殊延误航班
        if exceptional_indices:
            exceptional_indices = exceptional_indices.intersection(filtered_data.index)
            if exceptional_indices:
                filtered_data = filtered_data.drop(exceptional_indices)
                print(f"排除特殊延误航班: {len(exceptional_indices)} 个航班")
        
        # 排除天气停飞期间的航班
        weather_excluded_count = 0
        for weather in weather_periods:
            weather_mask = (
                (filtered_data['计划离港时间'].dt.date == weather['date']) &
                (filtered_data['计划离港时间'] >= weather['suspend_start']) &
                (filtered_data['计划离港时间'] <= weather['suspend_end'])
            )
            weather_excluded_count += weather_mask.sum()
            filtered_data = filtered_data[~weather_mask]
        
        print(f"排除天气停飞期间 {weather_excluded_count} 个航班")
        print(f"用于积压分析的有效航班数: {len(filtered_data)}")
        
        # 在过滤后的数据上进行积压分析
        filtered_data['小时'] = filtered_data['计划离港时间'].dt.hour
        filtered_data['日期'] = filtered_data['计划离港时间'].dt.date
        filtered_data['延误标记'] = filtered_data['起飞延误分钟'] > threshold
        
        # 按小时统计延误情况
        hourly_stats = filtered_data.groupby(['日期', '小时']).agg({
            '延误标记': ['count', 'sum'],
            '起飞延误分钟': 'mean'
        }).round(2)
        
        hourly_stats.columns = ['航班数', '延误航班数', '平均延误']
        hourly_stats = hourly_stats.reset_index()
        
        # 识别积压时段 - 使用动态阈值
        total_days = len(filtered_data['日期'].unique())
        dynamic_threshold = max(2, self.backlog_threshold / total_days)  # 至少2班延误
        
        backlog_periods = hourly_stats[
            hourly_stats['延误航班数'] >= dynamic_threshold
        ]
        
        print(f"\n积压识别结果（动态阈值: {dynamic_threshold:.1f}班/小时）:")
        print(f"识别到 {len(backlog_periods)} 个积压时段")
        
        if len(backlog_periods) > 0:
            backlog_summary = backlog_periods.groupby('小时').agg({
                '延误航班数': ['count', 'mean', 'sum']
            }).round(1)
            backlog_summary.columns = ['出现天数', '日均延误班数', '总延误班数']
            
            print("\n积压时段分布:")
            print("时段    出现天数  日均延误班数  总延误班数")
            print("-" * 40)
            for hour in sorted(backlog_summary.index):
                stats = backlog_summary.loc[hour]
                print(f"{hour:02d}:00  {stats['出现天数']:6.0f}    {stats['日均延误班数']:8.1f}    {stats['总延误班数']:8.0f}")
        
        return {
            'filtered_data': filtered_data,
            'backlog_periods': backlog_periods,
            'weather_periods': weather_periods,
            'exceptional_flights': exceptional_flights,
            'problematic_hours': problematic_hours,
            'threshold': threshold,
            'dynamic_threshold': dynamic_threshold
        }
    
    def analyze_daily_congestion_patterns(self, threshold=20):
        """每日积压模式分析"""
        print(f"\n=== 每日积压模式分析（延误阈值: {threshold}分钟）===")
        
        data = self.data.copy()
        data['小时'] = data['计划离港时间'].dt.hour
        data['日期'] = data['计划离港时间'].dt.date
        data['延误标记'] = data['起飞延误分钟'] > threshold
        
        # 按日期和小时统计
        daily_hourly_stats = data.groupby(['日期', '小时']).agg({
            '延误标记': ['count', 'sum'],
            '起飞延误分钟': 'mean'
        }).round(2)
        
        daily_hourly_stats.columns = ['航班数', '延误航班数', '平均延误']
        daily_hourly_stats = daily_hourly_stats.reset_index()
        
        # 识别每日的积压时段
        daily_backlog_summary = []
        total_days = len(data['日期'].unique())
        
        print(f"分析 {total_days} 天的数据...")
        print("\n每日积压时段识别:")
        print("日期        积压时段                     积压航班数")
        print("-" * 55)
        
        for date in sorted(data['日期'].unique()):
            day_data = daily_hourly_stats[daily_hourly_stats['日期'] == date]
            day_backlog = day_data[day_data['延误航班数'] >= self.backlog_threshold]
            
            if len(day_backlog) > 0:
                backlog_hours = sorted(day_backlog['小时'].tolist())
                total_backlog_flights = day_backlog['延误航班数'].sum()
                
                # 格式化积压时段显示
                if len(backlog_hours) == 1:
                    hours_str = f"{backlog_hours[0]:02d}:00"
                elif len(backlog_hours) <= 3:
                    hours_str = ", ".join([f"{h:02d}:00" for h in backlog_hours])
                else:
                    hours_str = f"{backlog_hours[0]:02d}:00-{backlog_hours[-1]:02d}:00等{len(backlog_hours)}个时段"
                
                print(f"{date}  {hours_str:<25}  {total_backlog_flights:>8}班")
                
                daily_backlog_summary.append({
                    'date': date,
                    'backlog_hours': backlog_hours,
                    'backlog_periods': len(backlog_hours),
                    'total_backlog_flights': total_backlog_flights
                })
            else:
                print(f"{date}  无积压时段                    {'0':>8}班")
        
        # 统计分析
        if daily_backlog_summary:
            avg_backlog_periods = np.mean([d['backlog_periods'] for d in daily_backlog_summary])
            avg_backlog_flights = np.mean([d['total_backlog_flights'] for d in daily_backlog_summary])
            backlog_days = len(daily_backlog_summary)
            
            print(f"\n=== 每日积压统计 ===")
            print(f"有积压的天数: {backlog_days}/{total_days} 天 ({backlog_days/total_days*100:.1f}%)")
            print(f"平均每天积压时段数: {avg_backlog_periods:.1f} 个")
            print(f"平均每天积压航班数: {avg_backlog_flights:.1f} 班")
            
            # 分析积压时段的时间分布
            all_backlog_hours = []
            for summary in daily_backlog_summary:
                all_backlog_hours.extend(summary['backlog_hours'])
            
            from collections import Counter
            hour_frequency = Counter(all_backlog_hours)
            
            print(f"\n积压时段频率分析:")
            print("时段    出现次数  占积压天数比例")
            print("-" * 30)
            for hour in sorted(hour_frequency.keys()):
                frequency = hour_frequency[hour]
                percentage = frequency / backlog_days * 100
                print(f"{hour:02d}:00   {frequency:6d}      {percentage:6.1f}%")
        else:
            print(f"\n=== 每日积压统计 ===")
            print("分析期间无积压时段")
        
        return daily_backlog_summary
    
    def calculate_delay_with_weather(self, data, weather_periods):
        """计算考虑天气停飞影响的延误时间"""
        delays = []
        
        for idx, flight in data.iterrows():
            planned_departure = flight['计划离港时间']
            actual_takeoff = flight['实际起飞时间']
            
            if pd.isna(actual_takeoff) or pd.isna(planned_departure):
                delays.append(np.nan)
                continue
            
            # 检查是否受天气停飞影响
            affected_by_weather = False
            reference_time = planned_departure  # 默认参考时间
            
            for period in weather_periods:
                if (flight['计划离港时间'].date() == period['date'] and
                    planned_departure >= period['suspend_start'] and 
                    planned_departure <= period['suspend_end']):
                    
                    # 受天气影响的航班，使用停飞结束时间作为参考
                    reference_time = period['suspend_end']
                    affected_by_weather = True
                    break
            
            # 计算延误：实际起飞时间 - 参考时间
            delay_minutes = (actual_takeoff - reference_time).total_seconds() / 60
            delays.append(delay_minutes)
        
        return delays
    
    def load_data(self):
        """载入ZGGG航班数据"""
        print(f"\n=== 载入航班数据 ===")
        
        # 读取数据
        df = pd.read_excel('/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/5月航班运行数据（脱敏）.xlsx')
        print(f"原始数据总记录数: {len(df)}")
        
        # 筛选ZGGG起飞航班
        zggg_flights = df[df['计划起飞站四字码'] == 'ZGGG'].copy()
        print(f"ZGGG起飞航班: {len(zggg_flights)} 班")
        
        # 数据清理
        required_cols = ['航班号', '计划离港时间', '实际离港时间', '实际起飞时间']
        valid_data = zggg_flights.dropna(subset=['航班号', '计划离港时间']).copy()
        print(f"有基本数据的航班: {len(valid_data)} 班")
        
        # 时间格式转换
        time_cols = ['计划离港时间', '实际离港时间', '实际起飞时间']
        for col in time_cols:
            if col in valid_data.columns:
                valid_data[col] = pd.to_datetime(valid_data[col], errors='coerce')
        
        # 处理缺失的起飞时间：用离港时间+20分钟估算
        missing_takeoff = valid_data['实际起飞时间'].isna()
        if missing_takeoff.sum() > 0:
            print(f"缺失实际起飞时间的航班: {missing_takeoff.sum()} 班")
            # 对于有离港时间但没有起飞时间的，用离港时间+20分钟估算
            valid_data.loc[missing_takeoff & valid_data['实际离港时间'].notna(), '实际起飞时间'] = (
                valid_data.loc[missing_takeoff & valid_data['实际离港时间'].notna(), '实际离港时间'] + 
                pd.Timedelta(minutes=20)
            )
            print(f"已为 {(missing_takeoff & valid_data['实际离港时间'].notna()).sum()} 班航班估算起飞时间")
        
        # 识别天气停飞时段
        weather_periods = self.identify_weather_suspension_periods(valid_data)
        
        # 计算起飞延误（考虑天气停飞影响）
        valid_data['起飞延误分钟'] = self.calculate_delay_with_weather(valid_data, weather_periods)
        
        print("使用起飞时间计算延误，已考虑天气停飞影响")
        
        # 过滤异常数据
        valid_data = valid_data[
            (valid_data['起飞延误分钟'] >= -60) &  # 延误不超过60分钟提前
            (valid_data['起飞延误分钟'] <= 600)     # 延误不超过10小时
        ]
        
        self.data = valid_data
        print(f"最终有效数据: {len(self.data)} 班")
        
        # 基本统计
        print(f"\n=== 基本统计信息 ===")
        print(f"平均延误: {self.data['起飞延误分钟'].mean():.1f} 分钟")
        print(f"延误标准差: {self.data['起飞延误分钟'].std():.1f} 分钟")
        print(f"延误中位数: {self.data['起飞延误分钟'].median():.1f} 分钟")
        
        return self.data
    
    def analyze_delay_distribution(self):
        """分析延误分布特征"""
        print(f"\n=== 延误分布分析 ===")
        
        # 延误分布统计
        delay_stats = {
            '提前（<0分钟）': (self.data['起飞延误分钟'] < 0).sum(),
            '准点（0-15分钟）': ((self.data['起飞延误分钟'] >= 0) & 
                            (self.data['起飞延误分钟'] <= 15)).sum(),
            '轻微延误（15-30分钟）': ((self.data['起飞延误分钟'] > 15) & 
                                (self.data['起飞延误分钟'] <= 30)).sum(),
            '中等延误（30-60分钟）': ((self.data['起飞延误分钟'] > 30) & 
                                (self.data['起飞延误分钟'] <= 60)).sum(),
            '严重延误（60-120分钟）': ((self.data['起飞延误分钟'] > 60) & 
                                 (self.data['起飞延误分钟'] <= 120)).sum(),
            '极端延误（>120分钟）': (self.data['起飞延误分钟'] > 120).sum()
        }
        
        total_flights = len(self.data)
        print("延误分布:")
        for category, count in delay_stats.items():
            percentage = count / total_flights * 100
            print(f"  {category}: {count} 班 ({percentage:.1f}%)")
        
        return delay_stats
    
    def test_different_thresholds(self):
        """测试不同延误判定阈值对积压时段的影响"""
        print(f"\n=== 测试不同延误判定阈值 ===")
        
        # 测试多个延误阈值
        thresholds = [5, 10, 15, 20, 25, 30, 40, 50, 60, 90, 120]
        results = []
        
        # 添加时间特征
        data = self.data.copy()
        if '计划离港时间' in data.columns:
            data['小时'] = data['计划离港时间'].dt.hour
            data['日期'] = data['计划离港时间'].dt.date
        
        for threshold in thresholds:
            # 标记延误航班
            data['延误标记'] = data['起飞延误分钟'] > threshold
            
            # 按小时统计每天的延误航班数
            hourly_stats = data.groupby(['日期', '小时']).agg({
                '延误标记': ['count', 'sum']
            })
            hourly_stats.columns = ['航班数', '延误航班数']
            hourly_stats = hourly_stats.reset_index()
            
            # 识别积压时段
            backlog_periods = hourly_stats[
                hourly_stats['延误航班数'] >= self.backlog_threshold
            ]
            
            # 统计积压时段小时数
            if len(backlog_periods) > 0:
                backlog_hours = sorted(backlog_periods['小时'].unique())
                backlog_hours_count = len(backlog_hours)
                backlog_periods_count = len(backlog_periods)
                
                # 计算平均每天积压小时数
                total_days = len(data['日期'].unique())
                avg_backlog_hours_per_day = backlog_periods_count / total_days
            else:
                backlog_hours = []
                backlog_hours_count = 0
                backlog_periods_count = 0
                avg_backlog_hours_per_day = 0
            
            # 计算延误航班比例
            delayed_flights = (data['起飞延误分钟'] > threshold).sum()
            delayed_ratio = delayed_flights / len(data) * 100
            
            results.append({
                'threshold': threshold,
                'delayed_flights': delayed_flights,
                'delayed_ratio': delayed_ratio,
                'backlog_periods': backlog_periods_count,
                'backlog_hours': backlog_hours,
                'backlog_hours_count': backlog_hours_count,
                'avg_backlog_hours_per_day': avg_backlog_hours_per_day
            })
            
            print(f"延误阈值 {threshold:3d}分钟: "
                  f"延误航班 {delayed_flights:4d}班({delayed_ratio:5.1f}%) "
                  f"积压时段 {backlog_periods_count:3d}个 "
                  f"涉及小时 {backlog_hours_count:2d}个 "
                  f"日均积压 {avg_backlog_hours_per_day:.1f}小时")
        
        return results
    
    def analyze_hourly_patterns(self, threshold=15):
        """分析不同时段的延误模式"""
        print(f"\n=== 小时级延误模式分析（延误阈值: {threshold}分钟）===")
        
        data = self.data.copy()
        data['小时'] = data['计划离港时间'].dt.hour
        data['日期'] = data['计划离港时间'].dt.date
        
        data['延误标记'] = data['起飞延误分钟'] > threshold
        
        # 按小时分组统计
        hourly_summary = data.groupby('小时').agg({
            '起飞延误分钟': ['count', 'mean', 'std', 'median'],
            '延误标记': ['sum', 'mean']
        }).round(2)
        
        hourly_summary.columns = ['航班数', '平均延误', '延误标准差', '延误中位数', '延误航班数', '延误率']
        
        # 识别潜在的积压时段
        potential_backlog_hours = hourly_summary[
            hourly_summary['延误航班数'] >= self.backlog_threshold / len(data['日期'].unique())
        ].index.tolist()
        
        print("各小时延误情况:")
        print("时段    航班数  平均延误  延误率    延误航班数  是否积压")
        print("-" * 55)
        
        for hour in range(24):
            if hour in hourly_summary.index:
                stats = hourly_summary.loc[hour]
                is_backlog = "是" if hour in potential_backlog_hours else "否"
                print(f"{hour:02d}:00  {stats['航班数']:6.0f}  {stats['平均延误']:7.1f}  "
                      f"{stats['延误率']*100:6.1f}%  {stats['延误航班数']:8.0f}   {is_backlog}")
            else:
                print(f"{hour:02d}:00      0      0.0     0.0%         0   否")
        
        return hourly_summary
    
    def find_optimal_threshold(self):
        """寻找最佳的延误判定阈值"""
        print(f"\n=== 寻找最佳延误判定阈值（积压门槛固定为{self.backlog_threshold}班）===")
        
        # 目标：积压时段应该符合航空运营规律
        # 理想情况：早高峰(7-9)、晚高峰(18-21)、可能的午间忙碌(12-14)
        expected_busy_hours = [7, 8, 9, 12, 13, 14, 18, 19, 20, 21]
        
        results = self.test_different_thresholds()
        
        print(f"\n评估标准:")
        print("1. 积压时段应该集中在繁忙时段，避免全天积压")
        print("2. 延误航班比例应该合理（15-40%）- 考虑天气影响调整范围")
        print("3. 积压时段应该有明显的时间集中性")
        print("4. 天气停飞恢复后的延误计算更合理")
        
        best_threshold = None
        best_score = -1
        
        for result in results:
            threshold = result['threshold']
            backlog_hours = result['backlog_hours']
            delayed_ratio = result['delayed_ratio']
            backlog_hours_count = result['backlog_hours_count']
            
            # 计算评分
            score = 0
            
            # 1. 延误比例合理性（15-40%比较合理，考虑天气影响）
            if 15 <= delayed_ratio <= 40:
                score += 30
            elif 10 <= delayed_ratio < 15 or 40 < delayed_ratio <= 50:
                score += 20
            elif delayed_ratio < 10 or delayed_ratio > 50:
                score += 0
            
            # 2. 积压时段数量合理性（避免全天积压）
            if 3 <= backlog_hours_count <= 8:
                score += 30
            elif 2 <= backlog_hours_count < 3 or 8 < backlog_hours_count <= 12:
                score += 20
            elif backlog_hours_count < 2 or backlog_hours_count > 12:
                score += 10
            
            # 3. 积压时段与繁忙时段的重合度
            overlap = len(set(backlog_hours) & set(expected_busy_hours))
            if overlap >= 4:
                score += 25
            elif overlap >= 2:
                score += 15
            elif overlap >= 1:
                score += 10
            
            # 4. 避免夜间积压（0-6点不应该有积压）
            night_backlog = len([h for h in backlog_hours if 0 <= h <= 6])
            if night_backlog == 0:
                score += 15
            elif night_backlog <= 2:
                score += 10
            
            print(f"阈值 {threshold:3d}分钟: 评分 {score:3d}/100 "
                  f"(延误率{delayed_ratio:5.1f}% 积压{backlog_hours_count:2d}小时 "
                  f"繁忙重合{overlap}个 夜间积压{night_backlog}个)")
            
            if score > best_score:
                best_score = score
                best_threshold = threshold
        
        print(f"\n🏆 推荐最佳延误判定阈值: {best_threshold} 分钟 (评分: {best_score}/100)")
        
        # 展示最佳阈值的详细分析
        if best_threshold:
            print(f"\n=== 使用最佳阈值 {best_threshold} 分钟的详细分析 ===")
            self.analyze_hourly_patterns(threshold=best_threshold)
            
        return best_threshold, best_score
    
    def visualize_delay_patterns(self, threshold=15):
        """可视化延误模式（使用过滤后的数据）"""
        print(f"\n=== 生成延误模式可视化图表 ===")
        
        # 首先进行数据过滤，排除系统性问题时段
        data = self.data.copy()
        
        # 识别系统性问题时段
        problematic_hours = self.identify_systematic_problematic_hours(data)
        
        # 排除系统性问题时段的数据
        if problematic_hours:
            problematic_hour_list = [h['hour'] for h in problematic_hours]
            original_count = len(data)
            data = data[~data['计划离港时间'].dt.hour.isin(problematic_hour_list)]
            excluded_count = original_count - len(data)
            print(f"可视化分析中排除系统性问题时段数据: {excluded_count} 个航班")
        
        data['小时'] = data['计划离港时间'].dt.hour
        data['日期'] = data['计划离港时间'].dt.date
        
        data['延误标记'] = data['起飞延误分钟'] > threshold
        
        # 创建图表
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle(f'ZGGG机场延误模式分析（延误阈值: {threshold}分钟）', fontsize=16)
        
        # 1. 每小时平均延误时间
        hourly_avg_delay = data.groupby('小时')['起飞延误分钟'].mean()
        axes[0,0].bar(hourly_avg_delay.index, hourly_avg_delay.values, color='skyblue')
        axes[0,0].set_title('各小时平均延误时间')
        axes[0,0].set_xlabel('小时')
        axes[0,0].set_ylabel('平均延误(分钟)')
        axes[0,0].grid(True, alpha=0.3)
        
        # 2. 每小时日均延误航班数量（修正）
        daily_hourly_delayed = data.groupby(['日期', '小时'])['延误标记'].sum().reset_index()
        hourly_avg_delayed = daily_hourly_delayed.groupby('小时')['延误标记'].mean()
        
        axes[0,1].bar(hourly_avg_delayed.index, hourly_avg_delayed.values, color='orange')
        axes[0,1].axhline(y=self.backlog_threshold, color='red', linestyle='--', 
                         label=f'积压阈值({self.backlog_threshold}班/小时)')
        axes[0,1].set_title('各小时日均延误航班数量')
        axes[0,1].set_xlabel('小时')
        axes[0,1].set_ylabel('日均延误航班数')
        axes[0,1].legend()
        axes[0,1].grid(True, alpha=0.3)
        
        # 3. 延误分布直方图
        axes[1,0].hist(data['起飞延误分钟'], bins=50, color='lightgreen', alpha=0.7)
        axes[1,0].axvline(x=threshold, color='red', linestyle='--', 
                         label=f'延误阈值({threshold}分钟)')
        axes[1,0].set_title('延误时间分布')
        axes[1,0].set_xlabel('延误时间(分钟)')
        axes[1,0].set_ylabel('航班数')
        axes[1,0].legend()
        axes[1,0].grid(True, alpha=0.3)
        
        # 4. 热力图：日期-小时延误航班数
        pivot_data = data.groupby(['日期', '小时'])['延误标记'].sum().reset_index()
        pivot_matrix = pivot_data.pivot(index='日期', columns='小时', values='延误标记')
        pivot_matrix = pivot_matrix.fillna(0)
        
        im = axes[1,1].imshow(pivot_matrix.values, cmap='YlOrRd', aspect='auto')
        axes[1,1].set_title('每日各小时延误航班数热力图')
        axes[1,1].set_xlabel('小时')
        axes[1,1].set_ylabel('日期')
        
        # 设置刻度
        axes[1,1].set_xticks(range(0, 24, 2))
        axes[1,1].set_xticklabels(range(0, 24, 2))
        
        plt.colorbar(im, ax=axes[1,1], label='延误航班数')
        plt.tight_layout()
        
        # 保存图表
        filename = f'ZGGG每日积压模式分析_{threshold}分钟阈值.png'
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"图表已保存为: {filename}")
        plt.show()

def main():
    """主函数"""
    analyzer = ZGGGBacklogAnalyzer()
    
    # 1. 载入数据
    data = analyzer.load_data()
    if data is None or len(data) == 0:
        print("❌ 数据载入失败")
        return
    
    # 2. 分析延误分布
    analyzer.analyze_delay_distribution()
    
    # 3. 测试不同阈值
    analyzer.test_different_thresholds()
    
    # 4. 寻找最佳阈值
    best_threshold, best_score = analyzer.find_optimal_threshold()
    
    # 5. 高级积压分析（排除特殊情况）
    if best_threshold:
        congestion_analysis = analyzer.identify_congestion_periods_advanced(threshold=best_threshold)
        print(f"\n=== 使用最佳阈值 {best_threshold} 分钟进行积压分析 ===")
        
        # 每日积压模式分析
        daily_backlog = analyzer.analyze_daily_congestion_patterns(threshold=best_threshold)
    
    # 6. 生成可视化图表
    if best_threshold:
        analyzer.visualize_delay_patterns(threshold=best_threshold)

if __name__ == "__main__":
    main()
