#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG机场积压时段优化分析
测试动态阈值和其他优化方案
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

class OptimizedBacklogAnalyzer:
    def __init__(self):
        """初始化优化分析器"""
        self.data = None
        self.backlog_threshold = 10
        
    def load_data(self):
        """载入数据"""
        df = pd.read_excel('/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/5月航班运行数据（脱敏）.xlsx')
        zggg_flights = df[df['计划起飞站四字码'] == 'ZGGG'].copy()
        
        # 时间格式转换和延误计算
        time_cols = ['计划离港时间', '实际离港时间']
        for col in time_cols:
            zggg_flights[col] = pd.to_datetime(zggg_flights[col], errors='coerce')
        
        zggg_flights['起飞延误分钟'] = (
            zggg_flights['实际离港时间'] - zggg_flights['计划离港时间']
        ).dt.total_seconds() / 60
        
        # 过滤异常数据
        self.data = zggg_flights[
            (zggg_flights['起飞延误分钟'] >= -60) &
            (zggg_flights['起飞延误分钟'] <= 600)
        ].copy()
        
        self.data['小时'] = self.data['计划离港时间'].dt.hour
        self.data['日期'] = self.data['计划离港时间'].dt.date
        
        print(f"载入ZGGG航班数据: {len(self.data)} 班")
        return self.data
    
    def test_dynamic_threshold(self):
        """测试动态阈值方案"""
        print(f"\n=== 方案A: 动态阈值测试 ===")
        
        # 动态阈值设定
        dynamic_thresholds = {
            0: 30, 1: 30, 2: 30, 3: 30, 4: 30, 5: 30,  # 夜间
            6: 20, 7: 20, 8: 20, 9: 20, 10: 20, 11: 20,  # 上午
            12: 20, 13: 20, 14: 20, 15: 20, 16: 20, 17: 20, 18: 20, 19: 20, 20: 20, 21: 20,  # 日间
            22: 25, 23: 25  # 深夜
        }
        
        data = self.data.copy()
        
        # 应用动态阈值
        data['动态延误阈值'] = data['小时'].map(dynamic_thresholds)
        data['动态延误标记'] = data['起飞延误分钟'] > data['动态延误阈值']
        
        # 统计积压时段
        hourly_stats = data.groupby(['日期', '小时']).agg({
            '动态延误标记': ['count', 'sum'],
            '动态延误阈值': 'first'
        })
        hourly_stats.columns = ['航班数', '延误航班数', '阈值']
        hourly_stats = hourly_stats.reset_index()
        
        backlog_periods = hourly_stats[hourly_stats['延误航班数'] >= self.backlog_threshold]
        
        if len(backlog_periods) > 0:
            backlog_hours = sorted(backlog_periods['小时'].unique())
            backlog_count = len(backlog_periods)
            delayed_flights = data['动态延误标记'].sum()
            delayed_ratio = delayed_flights / len(data) * 100
            
            print(f"动态阈值方案结果:")
            print(f"  延误航班: {delayed_flights} 班 ({delayed_ratio:.1f}%)")
            print(f"  积压时段: {backlog_count} 个")
            print(f"  积压小时: {backlog_hours}")
            print(f"  涉及小时数: {len(backlog_hours)} 个")
            
            return {
                'method': 'dynamic',
                'backlog_hours': backlog_hours,
                'backlog_count': backlog_count,
                'delayed_ratio': delayed_ratio
            }
        else:
            print("动态阈值方案：无积压时段")
            return {'method': 'dynamic', 'backlog_hours': [], 'backlog_count': 0}
    
    def test_relative_threshold(self):
        """测试相对阈值方案"""
        print(f"\n=== 方案B: 相对阈值测试 ===")
        
        data = self.data.copy()
        
        # 计算每小时的相对阈值（平均值 + 0.5个标准差）
        hourly_stats = data.groupby('小时')['起飞延误分钟'].agg(['mean', 'std']).fillna(0)
        hourly_stats['相对阈值'] = hourly_stats['mean'] + 0.5 * hourly_stats['std']
        
        # 确保最小阈值为10分钟
        hourly_stats['相对阈值'] = hourly_stats['相对阈值'].clip(lower=10)
        
        print("各小时相对阈值:")
        for hour in range(24):
            if hour in hourly_stats.index:
                threshold = hourly_stats.loc[hour, '相对阈值']
                print(f"  {hour:02d}:00 - 阈值: {threshold:.1f}分钟")
        
        # 应用相对阈值
        data['相对阈值'] = data['小时'].map(hourly_stats['相对阈值'])
        data['相对延误标记'] = data['起飞延误分钟'] > data['相对阈值']
        
        # 统计积压时段
        hourly_backlog = data.groupby(['日期', '小时']).agg({
            '相对延误标记': ['count', 'sum']
        })
        hourly_backlog.columns = ['航班数', '延误航班数']
        hourly_backlog = hourly_backlog.reset_index()
        
        backlog_periods = hourly_backlog[hourly_backlog['延误航班数'] >= self.backlog_threshold]
        
        if len(backlog_periods) > 0:
            backlog_hours = sorted(backlog_periods['小时'].unique())
            backlog_count = len(backlog_periods)
            delayed_flights = data['相对延误标记'].sum()
            delayed_ratio = delayed_flights / len(data) * 100
            
            print(f"\n相对阈值方案结果:")
            print(f"  延误航班: {delayed_flights} 班 ({delayed_ratio:.1f}%)")
            print(f"  积压时段: {backlog_count} 个")
            print(f"  积压小时: {backlog_hours}")
            print(f"  涉及小时数: {len(backlog_hours)} 个")
            
            return {
                'method': 'relative',
                'backlog_hours': backlog_hours,
                'backlog_count': backlog_count,
                'delayed_ratio': delayed_ratio
            }
        else:
            print("相对阈值方案：无积压时段")
            return {'method': 'relative', 'backlog_hours': [], 'backlog_count': 0}
    
    def test_higher_backlog_threshold(self):
        """测试提高积压判定门槛"""
        print(f"\n=== 方案C: 提高积压判定门槛测试 ===")
        
        data = self.data.copy()
        data['延误标记'] = data['起飞延误分钟'] > 15  # 使用15分钟阈值
        
        thresholds = [15, 20, 25, 30]
        results = []
        
        for threshold in thresholds:
            hourly_stats = data.groupby(['日期', '小时']).agg({
                '延误标记': ['count', 'sum']
            })
            hourly_stats.columns = ['航班数', '延误航班数']
            hourly_stats = hourly_stats.reset_index()
            
            backlog_periods = hourly_stats[hourly_stats['延误航班数'] >= threshold]
            
            if len(backlog_periods) > 0:
                backlog_hours = sorted(backlog_periods['小时'].unique())
                backlog_count = len(backlog_periods)
                
                results.append({
                    'threshold': threshold,
                    'backlog_hours': backlog_hours,
                    'backlog_count': backlog_count,
                    'hour_count': len(backlog_hours)
                })
                
                print(f"积压门槛 {threshold:2d}班: 积压时段 {backlog_count:3d}个, "
                      f"涉及小时 {len(backlog_hours):2d}个, 小时分布 {backlog_hours}")
            else:
                print(f"积压门槛 {threshold:2d}班: 无积压时段")
        
        return results
    
    def compare_all_methods(self):
        """综合比较所有方案"""
        print(f"\n=== 方案比较总结 ===")
        
        # 原始方案（15分钟固定阈值）
        data = self.data.copy()
        data['延误标记'] = data['起飞延误分钟'] > 15
        hourly_stats = data.groupby(['日期', '小时']).agg({
            '延误标记': ['count', 'sum']
        })
        hourly_stats.columns = ['航班数', '延误航班数']
        hourly_stats = hourly_stats.reset_index()
        original_backlog = hourly_stats[hourly_stats['延误航班数'] >= 10]
        
        print(f"原始方案(15分钟固定):")
        print(f"  积压小时: {len(original_backlog['小时'].unique())}个")
        print(f"  积压时段: {len(original_backlog)}个")
        
        # 测试各方案
        dynamic_result = self.test_dynamic_threshold()
        relative_result = self.test_relative_threshold()
        higher_threshold_results = self.test_higher_backlog_threshold()
        
        print(f"\n=== 推荐方案 ===")
        
        # 评估最佳方案
        if dynamic_result['backlog_count'] > 0:
            dynamic_hours = len(dynamic_result['backlog_hours'])
            print(f"1. 动态阈值方案: 积压{dynamic_hours}小时 - 避免了夜间过度积压")
            
        if relative_result['backlog_count'] > 0:
            relative_hours = len(relative_result['backlog_hours'])
            print(f"2. 相对阈值方案: 积压{relative_hours}小时 - 基于统计特征自适应")
            
        print(f"3. 提高门槛方案: 推荐使用20班作为积压门槛")
        
        return {
            'original': len(original_backlog['小时'].unique()),
            'dynamic': dynamic_result,
            'relative': relative_result,
            'higher_threshold': higher_threshold_results
        }

def main():
    analyzer = OptimizedBacklogAnalyzer()
    analyzer.load_data()
    results = analyzer.compare_all_methods()

if __name__ == "__main__":
    main()
