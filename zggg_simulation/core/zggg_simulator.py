#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG机场核心仿真引擎
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.parameters import DEFAULT_CONFIG
from core.aircraft_classifier import AircraftClassifier
from core.runway_scheduler import RunwayScheduler
from utils.time_utils import TimeUtils

class ZGGGAirportSimulator:
    """ZGGG机场仿真核心引擎"""
    
    def __init__(self, config=None):
        """
        初始化仿真引擎
        
        Args:
            config: 仿真配置对象
        """
        self.config = config or DEFAULT_CONFIG
        self.aircraft_classifier = AircraftClassifier(self.config)
        self.runway_scheduler = RunwayScheduler(self.config)
        self.time_utils = TimeUtils()
        
        # 仿真结果存储
        self.simulation_results = {
            'departures': [],
            'arrivals': [],
            'statistics': {}
        }
        
        # 原始数据
        self.flight_data = None
        
    def load_flight_data(self, df: pd.DataFrame):
        """
        加载航班数据
        
        Args:
            df: 预处理后的ZGGG航班数据
        """
        print("正在加载航班数据...")
        
        # 保存原始数据
        self.flight_data = df.copy()
        
        # 添加飞机分类
        self.flight_data = self.aircraft_classifier.classify_flight_data(
            self.flight_data, '机型'
        )
        
        print(f"成功加载 {len(self.flight_data)} 条航班数据")
        
        # 打印分类统计
        self.aircraft_classifier.print_classification_report(self.flight_data)
    
    def prepare_simulation_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        准备仿真数据，分离出港和入港航班
        
        Returns:
            tuple: (出港航班DataFrame, 入港航班DataFrame)
        """
        if self.flight_data is None:
            raise ValueError("请先加载航班数据")
        
        # 查找机场代码列
        departure_col = None
        arrival_col = None
        
        for col in ['起飞站', '出发机场', '起飞机场', '实际起飞站四字码']:
            if col in self.flight_data.columns:
                departure_col = col
                break
                
        for col in ['到达站', '到达机场', '降落机场', '实际到达站四字码']:
            if col in self.flight_data.columns:
                arrival_col = col
                break
        
        # 分离出港和入港航班
        departures = pd.DataFrame()
        arrivals = pd.DataFrame()
        
        if departure_col:
            departures = self.flight_data[
                self.flight_data[departure_col].str.contains('ZGGG', na=False)
            ].copy()
        
        if arrival_col:
            arrivals = self.flight_data[
                self.flight_data[arrival_col].str.contains('ZGGG', na=False)
            ].copy()
        
        print(f"分离数据: {len(departures)} 出港航班, {len(arrivals)} 入港航班")
        
        # 为航班添加飞机重量分类
        if len(departures) > 0:
            departures['weight_class'] = departures['机型'].apply(
                lambda x: self.aircraft_classifier.classify_aircraft(x)
            )
            
        if len(arrivals) > 0:
            arrivals['weight_class'] = arrivals['机型'].apply(
                lambda x: self.aircraft_classifier.classify_aircraft(x)
            )
        
        return departures, arrivals
    
    def simulate_departures(self, departure_flights: pd.DataFrame) -> List[Dict]:
        """
        仿真出港航班
        
        Args:
            departure_flights: 出港航班数据
            
        Returns:
            list: 出港仿真结果列表
        """
        print("开始仿真出港航班...")
        
        departure_results = []
        
        # 查找时间列，优先使用计划时间，如果没有则使用实际时间
        def find_time_column(preferred_names, fallback_names):
            for col in preferred_names:
                if col in departure_flights.columns:
                    return col
            for col in fallback_names:
                if col in departure_flights.columns:
                    return col
            return None
        
        # 按计划离港时间排序（如果没有计划时间，使用实际时间）
        departure_time_col = find_time_column(
            ['计划离港', '计划起飞', '计划起飞时间'], 
            ['实际起飞', '实际起飞时间', '实际离港']
        )
        
        if departure_time_col and len(departure_flights) > 0:
            departure_flights = departure_flights.sort_values(departure_time_col).copy()
            print(f"使用 '{departure_time_col}' 列对 {len(departure_flights)} 个出港航班排序")
        else:
            print("未找到出港时间列，无法排序")
            return departure_results
        
        for idx, flight in departure_flights.iterrows():
            # 准备航班信息
            flight_info = {
                'flight_id': flight.get('航班号', f'DEP_{idx}'),
                'planned_departure': flight.get(departure_time_col),
                'aircraft_weight': flight.get('weight_class', 'Medium'),
                'aircraft_type': flight.get('机型', 'Unknown')
            }
            
            # 调度航班
            result = self.runway_scheduler.schedule_departure(flight_info)
            
            # 如果调度失败，跳过此航班
            if result is None:
                continue
            
            # 添加原始数据信息
            result['原始索引'] = idx
            result['实际离港'] = flight.get('实际离港', None)
            result['实际起飞'] = flight.get('实际起飞', None)
            
            departure_results.append(result)
        
        print(f"出港仿真完成: {len(departure_results)} 个航班")
        
        return departure_results
    
    def simulate_arrivals(self, arrival_flights: pd.DataFrame) -> List[Dict]:
        """
        仿真入港航班
        
        Args:
            arrival_flights: 入港航班数据
            
        Returns:
            list: 入港仿真结果列表
        """
        print("开始仿真入港航班...")
        
        arrival_results = []
        
        # 查找时间列，优先使用计划时间，如果没有则使用实际时间
        def find_time_column(preferred_names, fallback_names):
            for col in preferred_names:
                if col in arrival_flights.columns:
                    return col
            for col in fallback_names:
                if col in arrival_flights.columns:
                    return col
            return None
        
        # 按计划到达时间排序（如果没有计划时间，使用实际时间）
        arrival_time_col = find_time_column(
            ['计划到达', '计划降落', '计划到达时间'], 
            ['实际降落', '实际落地时间', '实际到达']
        )
        
        if arrival_time_col and len(arrival_flights) > 0:
            arrival_flights = arrival_flights.sort_values(arrival_time_col).copy()
            print(f"使用 '{arrival_time_col}' 列对 {len(arrival_flights)} 个入港航班排序")
        else:
            print("未找到入港时间列，无法排序")
            return arrival_results
        
        for idx, flight in arrival_flights.iterrows():
            # 准备航班信息
            flight_info = {
                'flight_id': flight.get('航班号', f'ARR_{idx}'),
                'planned_arrival': flight.get(arrival_time_col),
                'aircraft_weight': flight.get('weight_class', 'Medium'),
                'aircraft_type': flight.get('机型', 'Unknown')
            }
            
            # 调度航班
            result = self.runway_scheduler.schedule_arrival(flight_info)
            
            # 如果调度失败，跳过此航班
            if result is None:
                continue
            
            # 添加原始数据信息
            result['原始索引'] = idx
            result['实际降落'] = flight.get('实际降落', None)
            result['实际到达'] = flight.get('实际到达', None)
            
            arrival_results.append(result)
        
        print(f"入港仿真完成: {len(arrival_results)} 个航班")
        
        return arrival_results
    
    def run_simulation(self) -> Dict:
        """
        运行完整仿真
        
        Returns:
            dict: 完整仿真结果
        """
        print("=== 开始ZGGG机场排队仿真 ===")
        
        # 重置调度器状态
        self.runway_scheduler.reset()
        
        # 准备数据
        departures_df, arrivals_df = self.prepare_simulation_data()
        
        # 执行仿真
        departure_results = []
        arrival_results = []
        
        if len(departures_df) > 0:
            departure_results = self.simulate_departures(departures_df)
        
        if len(arrivals_df) > 0:
            arrival_results = self.simulate_arrivals(arrivals_df)
        
        # 保存结果
        self.simulation_results = {
            'departures': departure_results,
            'arrivals': arrival_results,
            'statistics': self.runway_scheduler.get_statistics()
        }
        
        print("\n=== 仿真完成 ===")
        self.print_simulation_summary()
        
        return self.simulation_results
    
    def calculate_backlog_periods(self, results: List[Dict]) -> Dict:
        """
        计算积压时段
        
        Args:
            results: 仿真结果列表
            
        Returns:
            dict: 积压时段分析结果
        """
        if not results:
            return {}
        
        # 按小时分组统计延误航班
        hourly_delays = {}
        delay_threshold = self.config.time_parameters['delay_threshold']
        
        for result in results:
            if result['delay_minutes'] > delay_threshold:
                # 获取小时
                if 'actual_takeoff' in result:
                    hour = result['actual_takeoff'].hour
                elif 'actual_landing' in result:
                    hour = result['actual_landing'].hour
                else:
                    continue
                
                if hour not in hourly_delays:
                    hourly_delays[hour] = 0
                hourly_delays[hour] += 1
        
        # 找出积压时段
        backlog_threshold = self.config.time_parameters['backlog_threshold']
        backlog_hours = [hour for hour, count in hourly_delays.items() 
                        if count >= backlog_threshold]
        
        backlog_analysis = {
            'hourly_delays': hourly_delays,
            'backlog_hours': sorted(backlog_hours),
            'backlog_start': min(backlog_hours) if backlog_hours else None,
            'backlog_end': max(backlog_hours) if backlog_hours else None,
            'backlog_duration': len(backlog_hours),
            'max_hourly_backlog': max(hourly_delays.values()) if hourly_delays else 0,
            'max_backlog_hour': max(hourly_delays.keys(), key=hourly_delays.get) 
                               if hourly_delays else None
        }
        
        return backlog_analysis
    
    def print_simulation_summary(self):
        """打印仿真摘要"""
        stats = self.simulation_results['statistics']
        
        print("=== 仿真结果摘要 ===")
        print(f"总处理航班: {stats['total_scheduled']}")
        print(f"延误航班: {stats['total_delays']}")
        print(f"延误率: {stats.get('delay_rate', 0):.1%}")
        print(f"平均延误: {stats.get('average_delay', 0):.1f} 分钟")
        
        # 跑道利用率
        print("\n跑道利用率:")
        for runway, percentage in stats.get('runway_utilization_percentage', {}).items():
            count = stats['runway_utilization'][runway]
            print(f"  {runway}: {count} 次 ({percentage:.1f}%)")
        
        # 积压分析
        if self.simulation_results['departures']:
            backlog = self.calculate_backlog_periods(self.simulation_results['departures'])
            if backlog.get('backlog_hours'):
                print(f"\n出港积压时段: {backlog['backlog_start']}-{backlog['backlog_end']}点")
                print(f"积压持续: {backlog['backlog_duration']} 小时")
                print(f"最高峰积压: {backlog['max_hourly_backlog']} 架次/小时")
    
    def export_results_to_excel(self, filename: str = "zggg_simulation_results.xlsx"):
        """
        将仿真结果导出到Excel文件
        
        Args:
            filename: 输出文件名
        """
        print(f"正在导出仿真结果到 {filename}...")
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # 导出出港结果
            if self.simulation_results['departures']:
                dep_df = pd.DataFrame(self.simulation_results['departures'])
                dep_df.to_excel(writer, sheet_name='出港仿真结果', index=False)
            
            # 导出入港结果
            if self.simulation_results['arrivals']:
                arr_df = pd.DataFrame(self.simulation_results['arrivals'])
                arr_df.to_excel(writer, sheet_name='入港仿真结果', index=False)
            
            # 导出统计信息
            stats_data = []
            for key, value in self.simulation_results['statistics'].items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        stats_data.append({
                            '类别': key,
                            '指标': sub_key,
                            '数值': sub_value
                        })
                else:
                    stats_data.append({
                        '类别': '总体统计',
                        '指标': key,
                        '数值': value
                    })
            
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='统计信息', index=False)
        
        print(f"结果已导出到 {filename}")
    
    def get_results_summary(self) -> Dict:
        """
        获取结果摘要
        
        Returns:
            dict: 结果摘要字典
        """
        summary = {
            'departure_count': len(self.simulation_results['departures']),
            'arrival_count': len(self.simulation_results['arrivals']),
            'total_flights': len(self.simulation_results['departures']) + len(self.simulation_results['arrivals']),
            'statistics': self.simulation_results['statistics']
        }
        
        # 添加积压分析
        if self.simulation_results['departures']:
            summary['departure_backlog'] = self.calculate_backlog_periods(
                self.simulation_results['departures']
            )
        
        if self.simulation_results['arrivals']:
            summary['arrival_backlog'] = self.calculate_backlog_periods(
                self.simulation_results['arrivals']
            )
        
        return summary

# 创建仿真器实例
def create_simulator(config=None) -> ZGGGAirportSimulator:
    """创建仿真器实例"""
    return ZGGGAirportSimulator(config)
