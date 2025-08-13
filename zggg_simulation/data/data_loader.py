#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据加载器 - 负责从Excel文件加载和预处理航班数据
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import os

class FlightDataLoader:
    """航班数据加载器"""
    
    def __init__(self, data_file_path: str = None):
        """
        初始化数据加载器
        
        Args:
            data_file_path: 数据文件路径
        """
        self.data_file_path = data_file_path or "数据/5月航班运行数据（脱敏）.xlsx"
        self.raw_data = None
        self.zggg_data = None
        
    def load_raw_data(self) -> pd.DataFrame:
        """
        加载原始数据
        
        Returns:
            pd.DataFrame: 原始航班数据
        """
        try:
            print(f"正在加载数据文件: {self.data_file_path}")
            
            # 尝试读取Excel文件
            if self.data_file_path.endswith('.xlsx') or self.data_file_path.endswith('.xls'):
                # 先尝试读取第一个sheet
                self.raw_data = pd.read_excel(self.data_file_path)
            else:
                # 尝试CSV格式
                self.raw_data = pd.read_csv(self.data_file_path, encoding='utf-8')
            
            print(f"成功加载数据: {len(self.raw_data)} 条记录")
            print(f"数据列: {list(self.raw_data.columns)}")
            
            return self.raw_data
            
        except Exception as e:
            print(f"加载数据失败: {str(e)}")
            print("请检查文件路径和格式")
            return pd.DataFrame()
    
    def extract_zggg_data(self, df: pd.DataFrame = None) -> pd.DataFrame:
        """
        提取ZGGG机场相关数据
        
        Args:
            df: 输入数据，如果None则使用已加载的数据
            
        Returns:
            pd.DataFrame: ZGGG相关航班数据
        """
        if df is None:
            df = self.raw_data
            
        if df is None or len(df) == 0:
            print("没有可用数据，请先加载数据")
            return pd.DataFrame()
        
        # 查找可能的机场代码列
        possible_departure_columns = ['起飞站', '出发机场', '起飞机场', '始发站', 'DEPT', '实际起飞站四字码']
        possible_arrival_columns = ['到达站', '到达机场', '降落机场', '目的站', 'DEST', '实际到达站四字码']
        
        departure_col = None
        arrival_col = None
        
        for col in possible_departure_columns:
            if col in df.columns:
                departure_col = col
                break
                
        for col in possible_arrival_columns:
            if col in df.columns:
                arrival_col = col
                break
        
        if not departure_col and not arrival_col:
            print("未找到机场代码列，请检查数据格式")
            return pd.DataFrame()
        
        # 提取ZGGG相关数据
        zggg_filter = pd.Series([False] * len(df))
        
        if departure_col:
            zggg_departures = df[departure_col].str.contains('ZGGG', na=False)
            zggg_filter |= zggg_departures
            print(f"找到 {zggg_departures.sum()} 个ZGGG出港航班")
        
        if arrival_col:
            zggg_arrivals = df[arrival_col].str.contains('ZGGG', na=False)
            zggg_filter |= zggg_arrivals
            print(f"找到 {zggg_arrivals.sum()} 个ZGGG入港航班")
        
        self.zggg_data = df[zggg_filter].copy()
        print(f"总共提取 {len(self.zggg_data)} 条ZGGG相关记录")
        
        return self.zggg_data
    
    def preprocess_time_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        预处理时间列
        
        Args:
            df: 输入数据
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        df = df.copy()
        
        # 查找可能的时间列
        time_columns = {
            '计划离港': ['计划离港时间', '计划出港时间', '预定离港', 'STD'],
            '实际离港': ['实际离港时间', '实际出港时间', '实际离港', 'ATD'],
            '计划起飞': ['计划起飞时间', '预定起飞', 'ETD'],
            '实际起飞': ['实际起飞时间', '实际起飞', 'ATOT'],
            '计划到达': ['计划到达时间', '预定到达', 'STA'],
            '实际到达': ['实际到达时间', '实际到达', 'ATA'],
            '计划降落': ['计划降落时间', '预定降落'],
            '实际降落': ['实际降落时间', '实际落地时间', '实际降落', 'ALDT']
        }
        
        # 统一时间列名称
        for standard_name, possible_names in time_columns.items():
            for col_name in possible_names:
                if col_name in df.columns:
                    df[standard_name] = pd.to_datetime(df[col_name], errors='coerce')
                    break
        
        return df
    
    def calculate_taxi_times(self, df: pd.DataFrame) -> Dict:
        """
        计算滑行时间统计
        
        Args:
            df: 包含时间信息的航班数据
            
        Returns:
            dict: 滑行时间统计结果
        """
        stats = {
            'taxi_out': {'mean': 18, 'std': 5, 'count': 0},
            'taxi_in': {'mean': 18, 'std': 5, 'count': 0}
        }
        
        # 计算出港滑行时间 (实际起飞 - 实际离港)
        if '实际起飞' in df.columns and '实际离港' in df.columns:
            taxi_out_times = []
            for _, row in df.iterrows():
                if pd.notna(row['实际起飞']) and pd.notna(row['实际离港']):
                    diff = (row['实际起飞'] - row['实际离港']).total_seconds() / 60
                    if 0 < diff < 60:  # 合理范围：0-60分钟
                        taxi_out_times.append(diff)
            
            if taxi_out_times:
                stats['taxi_out'] = {
                    'mean': np.mean(taxi_out_times),
                    'std': np.std(taxi_out_times),
                    'count': len(taxi_out_times)
                }
        
        # 计算入港滑行时间 (实际到达 - 实际降落)
        if '实际到达' in df.columns and '实际降落' in df.columns:
            taxi_in_times = []
            for _, row in df.iterrows():
                if pd.notna(row['实际到达']) and pd.notna(row['实际降落']):
                    diff = (row['实际到达'] - row['实际降落']).total_seconds() / 60
                    if 0 < diff < 60:  # 合理范围：0-60分钟
                        taxi_in_times.append(diff)
            
            if taxi_in_times:
                stats['taxi_in'] = {
                    'mean': np.mean(taxi_in_times),
                    'std': np.std(taxi_in_times),
                    'count': len(taxi_in_times)
                }
        
        return stats
    
    def analyze_daily_operations(self, df: pd.DataFrame) -> Dict:
        """
        分析ZGGG日常运营统计
        
        Args:
            df: ZGGG航班数据
            
        Returns:
            dict: 运营统计结果
        """
        if len(df) == 0:
            return {}
        
        # 统计出港和入港航班
        departure_col = None
        arrival_col = None
        
        for col in ['起飞站', '出发机场']:
            if col in df.columns:
                departure_col = col
                break
                
        for col in ['到达站', '到达机场']:
            if col in df.columns:
                arrival_col = col
                break
        
        stats = {
            'total_flights': len(df),
            'departures': 0,
            'arrivals': 0,
            'departure_arrival_ratio': 0
        }
        
        if departure_col:
            departures = df[df[departure_col].str.contains('ZGGG', na=False)]
            stats['departures'] = len(departures)
        
        if arrival_col:
            arrivals = df[df[arrival_col].str.contains('ZGGG', na=False)]
            stats['arrivals'] = len(arrivals)
        
        if stats['arrivals'] > 0:
            stats['departure_arrival_ratio'] = stats['departures'] / stats['arrivals']
        
        # 按日期统计
        if '计划离港' in df.columns:
            df['日期'] = df['计划离港'].dt.date
            daily_stats = df.groupby('日期').size()
            stats['daily_average'] = daily_stats.mean()
            stats['daily_std'] = daily_stats.std()
            stats['max_daily'] = daily_stats.max()
            stats['min_daily'] = daily_stats.min()
        
        return stats
    
    def get_aircraft_types_distribution(self, df: pd.DataFrame) -> Dict:
        """
        获取机型分布统计
        
        Args:
            df: 航班数据
            
        Returns:
            dict: 机型分布统计
        """
        aircraft_col = None
        for col in ['机型', '飞机型号', 'ACTYPE']:
            if col in df.columns:
                aircraft_col = col
                break
        
        if not aircraft_col:
            return {}
        
        distribution = df[aircraft_col].value_counts()
        return {
            'top_10_types': distribution.head(10).to_dict(),
            'total_types': len(distribution),
            'most_common': distribution.index[0] if len(distribution) > 0 else None,
            'distribution_percentage': (distribution.head(10) / len(df) * 100).to_dict()
        }
    
    def print_data_summary(self):
        """打印数据摘要"""
        if self.zggg_data is None or len(self.zggg_data) == 0:
            print("没有可用的ZGGG数据")
            return
        
        print("=== ZGGG数据摘要 ===")
        print(f"数据时间范围: {self.zggg_data.index.min() if not self.zggg_data.empty else 'N/A'}")
        print(f"总记录数: {len(self.zggg_data)}")
        
        # 运营统计
        ops_stats = self.analyze_daily_operations(self.zggg_data)
        if ops_stats:
            print(f"出港航班: {ops_stats['departures']}")
            print(f"入港航班: {ops_stats['arrivals']}")
            print(f"出入港比例: {ops_stats['departure_arrival_ratio']:.2f}")
            if 'daily_average' in ops_stats:
                print(f"日均航班: {ops_stats['daily_average']:.1f}")
        
        # 滑行时间统计
        taxi_stats = self.calculate_taxi_times(self.zggg_data)
        print(f"出港滑行时间: {taxi_stats['taxi_out']['mean']:.1f}±{taxi_stats['taxi_out']['std']:.1f}分钟")
        print(f"入港滑行时间: {taxi_stats['taxi_in']['mean']:.1f}±{taxi_stats['taxi_in']['std']:.1f}分钟")
        
        # 机型分布
        aircraft_dist = self.get_aircraft_types_distribution(self.zggg_data)
        if aircraft_dist and 'top_10_types' in aircraft_dist:
            print("\n前5机型:")
            for aircraft, count in list(aircraft_dist['top_10_types'].items())[:5]:
                percentage = aircraft_dist['distribution_percentage'][aircraft]
                print(f"  {aircraft}: {count} ({percentage:.1f}%)")

# 创建默认数据加载器
def create_data_loader(data_path: str = None) -> FlightDataLoader:
    """创建数据加载器实例"""
    return FlightDataLoader(data_path)
