#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
跑道调度器 - 负责管理跑道资源和航班调度
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

class RunwayScheduler:
    """跑道调度器类"""
    
    def __init__(self, config):
        """
        初始化跑道调度器
        
        Args:
            config: 仿真配置对象
        """
        self.config = config
        
        # 跑道状态 - 记录每条跑道的最后使用时间
        self.runway_status = {
            'departure': {runway: datetime.min for runway in config.runway_config['departure_runways']},
            'arrival': {runway: datetime.min for runway in config.runway_config['arrival_runways']}
        }
        
        # 跑道队列 - 记录每条跑道上的飞机重量等级历史
        self.runway_aircraft_history = {
            'departure': {runway: [] for runway in config.runway_config['departure_runways']},
            'arrival': {runway: [] for runway in config.runway_config['arrival_runways']}
        }
        
        # 统计信息
        self.statistics = {
            'total_scheduled': 0,
            'total_delays': 0,
            'runway_utilization': defaultdict(int),
            'average_delay': 0
        }
    
    def reset(self):
        """重置调度器状态"""
        # 重置跑道状态
        for operation_type in self.runway_status:
            for runway in self.runway_status[operation_type]:
                self.runway_status[operation_type][runway] = datetime.min
        
        # 重置历史记录
        for operation_type in self.runway_aircraft_history:
            for runway in self.runway_aircraft_history[operation_type]:
                self.runway_aircraft_history[operation_type][runway] = []
        
        # 重置统计信息
        self.statistics = {
            'total_scheduled': 0,
            'total_delays': 0,
            'runway_utilization': defaultdict(int),
            'average_delay': 0
        }
    
    def get_optimal_runway(self, operation_type, planned_time, aircraft_weight):
        """选择最优跑道"""
        # 获取可用跑道
        available_runways = {}
        if operation_type == 'departure':
            for runway in self.config.runway_config['departure_runways']:
                available_runways[runway] = self.runway_status['departure'][runway]
        else:  # arrival
            for runway in self.config.runway_config['arrival_runways']:
                available_runways[runway] = self.runway_status['arrival'][runway]
        
        if not available_runways:
            return None
        
        # 过滤掉NaT值的跑道
        valid_runways = {}
        for runway, time in available_runways.items():
            if pd.notna(time) and time != pd.NaT:
                valid_runways[runway] = time
        
        if not valid_runways:
            return None
            
        # 选择最早可用的跑道
        best_runway = min(valid_runways.keys(), 
                         key=lambda r: valid_runways[r])
        return best_runway
    
    def _calculate_runway_available_time(self, last_busy_time: datetime, 
                                       previous_weight: Optional[str], 
                                       current_weight: str, 
                                       operation_type: str) -> datetime:
        """
        计算跑道可用时间
        
        Args:
            last_busy_time: 跑道最后繁忙时间
            previous_weight: 前一架飞机重量等级
            current_weight: 当前飞机重量等级
            operation_type: 操作类型
            
        Returns:
            datetime: 跑道可用时间
        """
        if last_busy_time == datetime.min:
            return datetime.min  # 跑道空闲
        
        # 获取ROT时间
        if operation_type == 'departure':
            rot_seconds = self.config.time_parameters['rot_takeoff']
        else:
            rot_seconds = self.config.time_parameters['rot_landing']
        
        # 获取尾流间隔
        wake_separation = 0
        if previous_weight:
            wake_separation = self.config.get_wake_separation(previous_weight, current_weight)
        
        # 跑道可用时间 = 最后使用时间 + ROT + 尾流间隔
        available_time = last_busy_time + timedelta(seconds=rot_seconds + wake_separation)
        
        return available_time
    
    def schedule_departure(self, flight_info: Dict) -> Dict:
        """
        调度出港航班
        
        Args:
            flight_info: 航班信息字典，包含：
                - planned_departure: 计划离港时间
                - aircraft_weight: 飞机重量等级
                - flight_id: 航班号
                
        Returns:
            dict: 调度结果，包含实际起飞时间、使用跑道等
        """
        planned_departure = flight_info['planned_departure']
        aircraft_weight = flight_info['aircraft_weight']
        flight_id = flight_info.get('flight_id', 'Unknown')
        
        # 计算计划起飞时间 = 计划离港时间 + 滑行时间
        taxi_out_mean = self.config.time_parameters['taxi_out_mean']
        taxi_out_std = self.config.time_parameters['taxi_out_std']
        
        # 添加随机滑行时间变化
        actual_taxi_time = np.random.normal(taxi_out_mean, taxi_out_std)
        actual_taxi_time = max(5, actual_taxi_time)  # 最少5分钟滑行时间
        
        planned_takeoff = planned_departure + timedelta(minutes=actual_taxi_time)
        
        # 选择最优跑道
        optimal_runway = self.get_optimal_runway('departure', planned_takeoff, aircraft_weight)
        
        # 如果无法找到可用跑道，跳过此航班
        if optimal_runway is None:
            print(f"警告: 无法为航班 {flight_id} 分配出港跑道，跳过此航班")
            return None
        
        # 计算实际起飞时间
        runway_available_time = self._calculate_runway_available_time(
            self.runway_status['departure'][optimal_runway],
            self.runway_aircraft_history['departure'][optimal_runway][-1] 
            if self.runway_aircraft_history['departure'][optimal_runway] else None,
            aircraft_weight,
            'departure'
        )
        
        # 实际起飞时间 = max(计划起飞时间, 跑道可用时间)
        actual_takeoff = max(planned_takeoff, runway_available_time)
        
        # 更新跑道状态
        self._update_runway_status(optimal_runway, actual_takeoff, aircraft_weight, 'departure')
        
        # 计算延误
        delay_minutes = (actual_takeoff - planned_takeoff).total_seconds() / 60
        
        # 更新统计信息
        self._update_statistics(delay_minutes, optimal_runway)
        
        return {
            'flight_id': flight_id,
            'planned_departure': planned_departure,
            'planned_takeoff': planned_takeoff,
            'actual_takeoff': actual_takeoff,
            'runway_used': optimal_runway,
            'aircraft_weight': aircraft_weight,
            'delay_minutes': delay_minutes,
            'taxi_out_minutes': actual_taxi_time
        }
    
    def schedule_arrival(self, flight_info: Dict) -> Dict:
        """
        调度入港航班
        
        Args:
            flight_info: 航班信息字典
            
        Returns:
            dict: 调度结果
        """
        planned_arrival = flight_info['planned_arrival']
        aircraft_weight = flight_info['aircraft_weight']
        flight_id = flight_info.get('flight_id', 'Unknown')
        
        # 选择最优跑道
        optimal_runway = self.get_optimal_runway('arrival', planned_arrival, aircraft_weight)
        
        # 如果无法找到可用跑道，跳过此航班
        if optimal_runway is None:
            print(f"警告: 无法为航班 {flight_id} 分配进港跑道，跳过此航班")
            return None
        
        # 计算实际降落时间
        runway_available_time = self._calculate_runway_available_time(
            self.runway_status['arrival'][optimal_runway],
            self.runway_aircraft_history['arrival'][optimal_runway][-1] 
            if self.runway_aircraft_history['arrival'][optimal_runway] else None,
            aircraft_weight,
            'arrival'
        )
        
        # 实际降落时间 = max(计划降落时间, 跑道可用时间)
        actual_landing = max(planned_arrival, runway_available_time)
        
        # 计算实际到达时间 = 实际降落时间 + 滑行时间
        taxi_in_mean = self.config.time_parameters['taxi_in_mean']
        taxi_in_std = self.config.time_parameters['taxi_in_std']
        actual_taxi_time = np.random.normal(taxi_in_mean, taxi_in_std)
        actual_taxi_time = max(5, actual_taxi_time)
        
        actual_arrival = actual_landing + timedelta(minutes=actual_taxi_time)
        
        # 更新跑道状态
        self._update_runway_status(optimal_runway, actual_landing, aircraft_weight, 'arrival')
        
        # 计算延误
        delay_minutes = (actual_landing - planned_arrival).total_seconds() / 60
        
        # 更新统计信息
        self._update_statistics(delay_minutes, optimal_runway)
        
        return {
            'flight_id': flight_id,
            'planned_arrival': planned_arrival,
            'actual_landing': actual_landing,
            'actual_arrival': actual_arrival,
            'runway_used': optimal_runway,
            'aircraft_weight': aircraft_weight,
            'delay_minutes': delay_minutes,
            'taxi_in_minutes': actual_taxi_time
        }
    
    def _update_runway_status(self, runway_id: str, operation_time: datetime, 
                            aircraft_weight: str, operation_type: str):
        """
        更新跑道状态
        
        Args:
            runway_id: 跑道ID
            operation_time: 操作时间
            aircraft_weight: 飞机重量等级
            operation_type: 操作类型
        """
        # 更新跑道最后使用时间
        self.runway_status[operation_type][runway_id] = operation_time
        
        # 更新飞机历史记录
        self.runway_aircraft_history[operation_type][runway_id].append(aircraft_weight)
        
        # 保留最近的历史记录（避免内存过多占用）
        if len(self.runway_aircraft_history[operation_type][runway_id]) > 100:
            self.runway_aircraft_history[operation_type][runway_id] = \
                self.runway_aircraft_history[operation_type][runway_id][-50:]
    
    def _update_statistics(self, delay_minutes: float, runway_id: str):
        """
        更新统计信息
        
        Args:
            delay_minutes: 延误分钟数
            runway_id: 使用的跑道ID
        """
        self.statistics['total_scheduled'] += 1
        
        if delay_minutes > 0:
            self.statistics['total_delays'] += 1
        
        self.statistics['runway_utilization'][runway_id] += 1
        
        # 更新平均延误
        total_delay_time = self.statistics.get('total_delay_time', 0) + delay_minutes
        self.statistics['total_delay_time'] = total_delay_time
        self.statistics['average_delay'] = total_delay_time / self.statistics['total_scheduled']
    
    def get_statistics(self) -> Dict:
        """
        获取调度统计信息
        
        Returns:
            dict: 统计信息
        """
        stats = self.statistics.copy()
        
        # 计算延误率
        if stats['total_scheduled'] > 0:
            stats['delay_rate'] = stats['total_delays'] / stats['total_scheduled']
        else:
            stats['delay_rate'] = 0
        
        # 计算跑道利用率分布
        total_operations = sum(stats['runway_utilization'].values())
        if total_operations > 0:
            stats['runway_utilization_percentage'] = {
                runway: (count / total_operations) * 100 
                for runway, count in stats['runway_utilization'].items()
            }
        else:
            stats['runway_utilization_percentage'] = {}
        
        return stats
    
    def print_statistics(self):
        """打印统计信息"""
        stats = self.get_statistics()
        
        print("=== 跑道调度统计 ===")
        print(f"总调度航班: {stats['total_scheduled']}")
        print(f"延误航班: {stats['total_delays']}")
        print(f"延误率: {stats['delay_rate']:.1%}")
        print(f"平均延误: {stats['average_delay']:.1f} 分钟")
        
        print("\n跑道利用率:")
        for runway, percentage in stats.get('runway_utilization_percentage', {}).items():
            count = stats['runway_utilization'][runway]
            print(f"  {runway}: {count} 次 ({percentage:.1f}%)")

# 创建调度器实例
def create_runway_scheduler(config) -> RunwayScheduler:
    """创建跑道调度器实例"""
    return RunwayScheduler(config)
