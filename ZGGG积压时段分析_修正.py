#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG机场积压时段分析（修正版）
新增系统性问题时段识别，专门处理早上5点等系统性延误异常
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
        
        print("=== ZGGG机场积压时段分析器（修正版）===")
        print(f"积压判定标准: 延误航班数 >= {self.backlog_threshold} 班/小时")
        print("新增功能: 系统性问题时段识别与过滤")
    
    def identify_weather_suspension_periods(self, data):
        """识别天气停飞时段"""
        print(f"\n=== 识别天气停飞时段 ===")
        
        weather_periods = []
        
        for date in data['计划离港时间'].dt.date.unique():
            day_data = data[data['计划离港时间'].dt.date == date].copy()
            day_data = day_data.sort_values('计划离港时间')
            
            for hour in range(24):
                hour_data = day_data[day_data['计划离港时间'].dt.hour == hour]
                if len(hour_data) == 0:
                    continue
                
                if '实际起飞时间' in hour_data.columns:
                    delays = (
                        hour_data['实际起飞时间'] - hour_data['计划离港时间']
                    ).dt.total_seconds() / 60
                    
                    severe_delays = (delays > 120).sum()
                    
                    if (len(hour_data) >= 3 and  
                        severe_delays / len(hour_data) > 0.8):  
                        
                        suspend_start = hour_data['计划离港时间'].min()
                        
                        suspend_end = None
                        for check_hour in range(hour, 24):
                            check_data = day_data[day_data['计划离港时间'].dt.hour == check_hour]
                            if len(check_data) > 0:
                                check_delays = (
                                    check_data['实际起飞时间'] - check_data['计划离港时间']
                                ).dt.total_seconds() / 60
                                normal_flights = (check_delays <= 60).sum()
                                
                                if len(check_data) > 0 and normal_flights / len(check_data) > 0.5:
                                    suspend_end = check_data['实际起飞时间'].min()
                                    break
                        
                        if suspend_end is None:
                            suspend_end = hour_data['实际起飞时间'].max()
                        
                        weather_periods.append({
                            'date': date,
                            'suspend_start': suspend_start,
                            'suspend_end': suspend_end,
                            'affected_flights': len(hour_data)
                        })
                        
                        print(f"发现天气停飞: {date} {suspend_start.strftime('%H:%M')}-{suspend_end.strftime('%H:%M')} 影响{len(hour_data)}班")
        
        print(f"识别到 {len(weather_periods)} 个天气停飞时段")
        return weather_periods
    
    def identify_exceptional_delays(self, data):
        """识别并标记特殊延误航班（个别情况），这些航班不应计入积压分析"""
        print(f"\n=== 识别特殊延误航班 ===")
        
        exceptional_flights = []
        
        for date in data['计划离港时间'].dt.date.unique():
            day_data = data[data['计划离港时间'].dt.date == date].copy()
            
            for hour in range(24):
                hour_data = day_data[day_data['计划离港时间'].dt.hour == hour]
                if len(hour_data) <= 1:
                    continue
                
                if '实际起飞时间' in hour_data.columns:
                    delays = (
                        hour_data['实际起飞时间'] - hour_data['计划离港时间']
                    ).dt.total_seconds() / 60
                    
                    normal_delays = (delays <= 60).sum()
                    severe_delays = (delays > 120).sum()
                    
                    if (len(hour_data) >= 3 and  
                        severe_delays <= 2 and  
                        normal_delays / len(hour_data) > 0.6):  
                        
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
                            print(f"标记特殊延误: {date} {hour:02d}:00时段 - 延误{flight_delay:.0f}分钟")
        
        print(f"识别到 {len(exceptional_flights)} 个特殊延误航班，将从积压分析中排除")
        return exceptional_flights
    
    def identify_systematic_problematic_hours(self, data):
        """识别系统性问题时段（整个小时段都有异常延误）"""
        print(f"\n=== 识别系统性问题时段 ===")
        
        problematic_hours = []
        
        # 分析每个小时的整体延误情况
        for hour in range(24):
            hour_data = data[data['计划离港时间'].dt.hour == hour]
            if len(hour_data) < 10:  # 样本太少，跳过
                continue
                
            if '实际起飞时间' in hour_data.columns:
                delays = (
                    hour_data['实际起飞时间'] - hour_data['计划离港时间']
                ).dt.total_seconds() / 60
                
                avg_delay = delays.mean()
                severe_delay_ratio = (delays > 120).sum() / len(delays) if len(delays) > 0 else 0
                
                # 系统性问题的判定条件：
                # 1. 平均延误超过240分钟（4小时）
                # 2. 严重延误比例超过60%
                # 3. 样本数量足够（>=10班）
                if (avg_delay > 240 and 
                    severe_delay_ratio > 0.6 and 
                    len(hour_data) >= 10):
                    
                    problematic_hours.append({
                        'hour': hour,
                        'avg_delay': avg_delay,
                        'severe_ratio': severe_delay_ratio,
                        'total_flights': len(hour_data)
                    })
                    
                    print(f"识别系统性问题时段: {hour:02d}:00 - 平均延误{avg_delay:.0f}分钟, "
                          f"严重延误比例{severe_delay_ratio:.1%}, 总航班{len(hour_data)}班")
        
        return problematic_hours
    
    def process_data(self):
        """处理数据，应用所有过滤条件"""
        print(f"\n=== 数据处理流程 ===")
        
        data = self.data.copy()
        original_count = len(data)
        
        # 识别天气停飞时段
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
        
        if weather_excluded_count > 0:
            print(f"排除天气停飞期间: {weather_excluded_count} 个航班")
        
        # 排除特殊延误航班
        if exceptional_flights:
            exceptional_indices = {flight['index'] for flight in exceptional_flights}
            exceptional_indices = exceptional_indices.intersection(filtered_data.index)
            if exceptional_indices:
                filtered_data = filtered_data.drop(exceptional_indices)
                print(f"排除特殊延误航班: {len(exceptional_indices)} 个航班")
        
        final_count = len(filtered_data)
        total_excluded = original_count - final_count
        
        print(f"\n数据处理结果:")
        print(f"原始数据: {original_count} 班")
        print(f"最终有效数据: {final_count} 班")
        print(f"总计排除: {total_excluded} 班 ({total_excluded/original_count*100:.1f}%)")
        
        return {
            'filtered_data': filtered_data,
            'weather_periods': weather_periods,
            'exceptional_flights': exceptional_flights,
            'problematic_hours': problematic_hours
        }
    
    def analyze_with_filters(self, threshold=15):
        """使用过滤条件进行积压分析"""
        print(f"\n=== 使用过滤条件进行积压分析（延误阈值: {threshold}分钟）===")
        
        # 处理数据
        process_result = self.process_data()
        filtered_data = process_result['filtered_data']
        
        if len(filtered_data) == 0:
            print("❌ 过滤后没有数据可分析")
            return None
        
        # 标记延误
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
        
        # 识别积压时段
        backlog_periods = hourly_stats[
            hourly_stats['延误航班数'] >= self.backlog_threshold
        ]
        
        print(f"\n积压分析结果:")
        print(f"有效数据: {len(filtered_data)} 班")
        print(f"识别积压时段: {len(backlog_periods)} 个")
        
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
            'process_result': process_result
        }
    
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
            reference_time = planned_departure  
            
            for period in weather_periods:
                if (flight['计划离港时间'].date() == period['date'] and
                    planned_departure >= period['suspend_start'] and 
                    planned_departure <= period['suspend_end']):
                    
                    reference_time = period['suspend_end']
                    break
            
            # 计算延误：实际起飞时间 - 参考时间
            delay_minutes = (actual_takeoff - reference_time).total_seconds() / 60
            delays.append(delay_minutes)
        
        return delays
    
    def load_data(self):
        """载入ZGGG航班数据"""
        print(f"\n=== 载入航班数据 ===")
        
        df = pd.read_excel('/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/5月航班运行数据（脱敏）.xlsx')
        print(f"原始数据总记录数: {len(df)}")
        
        zggg_flights = df[df['计划起飞站四字码'] == 'ZGGG'].copy()
        print(f"ZGGG起飞航班: {len(zggg_flights)} 班")
        
        valid_data = zggg_flights.dropna(subset=['航班号', '计划离港时间']).copy()
        print(f"有基本数据的航班: {len(valid_data)} 班")
        
        time_cols = ['计划离港时间', '实际离港时间', '实际起飞时间']
        for col in time_cols:
            if col in valid_data.columns:
                valid_data[col] = pd.to_datetime(valid_data[col], errors='coerce')
        
        # 处理缺失的起飞时间
        missing_takeoff = valid_data['实际起飞时间'].isna()
        if missing_takeoff.sum() > 0:
            print(f"缺失实际起飞时间的航班: {missing_takeoff.sum()} 班")
            valid_data.loc[missing_takeoff & valid_data['实际离港时间'].notna(), '实际起飞时间'] = (
                valid_data.loc[missing_takeoff & valid_data['实际离港时间'].notna(), '实际离港时间'] + 
                pd.Timedelta(minutes=20)
            )
            print(f"已为 {(missing_takeoff & valid_data['实际离港时间'].notna()).sum()} 班航班估算起飞时间")
        
        # 初步识别天气停飞时段（用于延误计算）
        weather_periods = self.identify_weather_suspension_periods(valid_data)
        
        # 计算起飞延误
        valid_data['起飞延误分钟'] = self.calculate_delay_with_weather(valid_data, weather_periods)
        
        # 过滤异常数据
        valid_data = valid_data[
            (valid_data['起飞延误分钟'] >= -60) &  
            (valid_data['起飞延误分钟'] <= 600)    
        ]
        
        self.data = valid_data
        print(f"最终有效数据: {len(self.data)} 班")
        
        # 基本统计
        print(f"\n=== 基本统计信息 ===")
        print(f"平均延误: {self.data['起飞延误分钟'].mean():.1f} 分钟")
        print(f"延误标准差: {self.data['起飞延误分钟'].std():.1f} 分钟")
        print(f"延误中位数: {self.data['起飞延误分钟'].median():.1f} 分钟")
        
        return self.data

def main():
    """主函数"""
    analyzer = ZGGGBacklogAnalyzer()
    
    # 载入数据
    data = analyzer.load_data()
    if data is None or len(data) == 0:
        print("❌ 数据载入失败")
        return
    
    # 使用修正后的方法进行分析
    print("=" * 60)
    print("使用修正版分析（增加系统性问题时段过滤）")
    print("=" * 60)
    
    # 使用最佳阈值进行分析
    result = analyzer.analyze_with_filters(threshold=15)
    
    if result:
        print("\n✅ 修正版分析完成")
        print("早上5点等系统性问题时段已被正确识别和过滤")
    
if __name__ == "__main__":
    main()
