#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指标计算器 - 计算四项验证指标
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

class MetricsCalculator:
    """验证指标计算器"""
    
    def __init__(self, config):
        """
        初始化指标计算器
        
        Args:
            config: 仿真配置对象
        """
        self.config = config
        self.delay_threshold = config.time_parameters['delay_threshold']
        self.backlog_threshold = config.time_parameters['backlog_threshold']
    
    def calculate_backlog_periods(self, results: List[Dict], actual_data: pd.DataFrame = None) -> Dict:
        """
        计算积压时段（仿真 vs 实际）
        
        Args:
            results: 仿真结果列表
            actual_data: 实际数据（可选，用于对比）
            
        Returns:
            dict: 积压时段计算结果
        """
        # 计算仿真积压时段
        sim_hourly_delays = self._calculate_hourly_delays(results, 'simulation')
        sim_backlog_hours = [hour for hour, count in sim_hourly_delays.items() 
                           if count >= self.backlog_threshold]
        
        sim_analysis = {
            'hourly_delays': sim_hourly_delays,
            'backlog_hours': sorted(sim_backlog_hours),
            'backlog_start': min(sim_backlog_hours) if sim_backlog_hours else None,
            'backlog_end': max(sim_backlog_hours) if sim_backlog_hours else None,
            'backlog_duration': len(sim_backlog_hours),
            'max_hourly_backlog': max(sim_hourly_delays.values()) if sim_hourly_delays else 0
        }
        
        # 如果有实际数据，计算实际积压时段
        actual_analysis = None
        if actual_data is not None:
            actual_hourly_delays = self._calculate_hourly_delays_from_actual(actual_data)
            actual_backlog_hours = [hour for hour, count in actual_hourly_delays.items() 
                                  if count >= self.backlog_threshold]
            
            actual_analysis = {
                'hourly_delays': actual_hourly_delays,
                'backlog_hours': sorted(actual_backlog_hours),
                'backlog_start': min(actual_backlog_hours) if actual_backlog_hours else None,
                'backlog_end': max(actual_backlog_hours) if actual_backlog_hours else None,
                'backlog_duration': len(actual_backlog_hours),
                'max_hourly_backlog': max(actual_hourly_delays.values()) if actual_hourly_delays else 0
            }
        
        return {
            'simulation': sim_analysis,
            'actual': actual_analysis
        }
    
    def _calculate_hourly_delays(self, results: List[Dict], data_type: str = 'simulation') -> Dict:
        """
        计算每小时延误航班数
        
        Args:
            results: 结果列表
            data_type: 数据类型标识
            
        Returns:
            dict: 每小时延误航班数
        """
        hourly_delays = {}
        
        for result in results:
            delay_minutes = result.get('delay_minutes', 0)
            
            if delay_minutes > self.delay_threshold:
                # 根据实际起飞/降落时间确定小时
                time_key = None
                if 'actual_takeoff' in result and result['actual_takeoff']:
                    time_key = result['actual_takeoff'].hour
                elif 'actual_landing' in result and result['actual_landing']:
                    time_key = result['actual_landing'].hour
                
                if time_key is not None:
                    if time_key not in hourly_delays:
                        hourly_delays[time_key] = 0
                    hourly_delays[time_key] += 1
        
        return hourly_delays
    
    def _calculate_hourly_delays_from_actual(self, actual_data: pd.DataFrame) -> Dict:
        """
        从实际数据计算每小时延误航班数
        
        Args:
            actual_data: 实际航班数据
            
        Returns:
            dict: 每小时延误航班数
        """
        hourly_delays = {}
        
        # 寻找时间列
        time_cols = ['实际起飞', '实际离港', '实际降落', '实际到达']
        planned_cols = ['计划起飞', '计划离港', '计划降落', '计划到达']
        
        for _, row in actual_data.iterrows():
            # 计算延误
            actual_time = None
            planned_time = None
            
            # 优先使用起飞数据（出港），其次使用降落数据（入港）
            for actual_col, planned_col in zip(time_cols, planned_cols):
                if actual_col in row and planned_col in row:
                    if pd.notna(row[actual_col]) and pd.notna(row[planned_col]):
                        actual_time = pd.to_datetime(row[actual_col])
                        planned_time = pd.to_datetime(row[planned_col])
                        break
            
            if actual_time and planned_time:
                delay_minutes = (actual_time - planned_time).total_seconds() / 60
                
                if delay_minutes > self.delay_threshold:
                    hour = actual_time.hour
                    if hour not in hourly_delays:
                        hourly_delays[hour] = 0
                    hourly_delays[hour] += 1
        
        return hourly_delays
    
    def calculate_metric_1_period_deviation(self, sim_backlog: Dict, actual_backlog: Dict) -> Dict:
        """
        指标1: 推演的积压时段与实际前后偏差不超过1个时段
        
        Args:
            sim_backlog: 仿真积压时段数据
            actual_backlog: 实际积压时段数据
            
        Returns:
            dict: 指标1计算结果
        """
        if not actual_backlog or actual_backlog['backlog_start'] is None:
            return {
                'passed': False,
                'reason': '无实际积压数据',
                'sim_period': None,
                'actual_period': None,
                'deviation': None
            }
        
        if not sim_backlog or sim_backlog['backlog_start'] is None:
            return {
                'passed': False,
                'reason': '仿真未发现积压',
                'sim_period': None,
                'actual_period': [actual_backlog['backlog_start'], actual_backlog['backlog_end']],
                'deviation': None
            }
        
        # 计算时段偏差
        start_deviation = abs(sim_backlog['backlog_start'] - actual_backlog['backlog_start'])
        end_deviation = abs(sim_backlog['backlog_end'] - actual_backlog['backlog_end'])
        max_deviation = max(start_deviation, end_deviation)
        
        tolerance = self.config.validation_parameters['time_tolerance_hours']
        passed = max_deviation <= tolerance
        
        return {
            'passed': passed,
            'reason': f"最大偏差 {max_deviation} 小时" if passed else f"偏差 {max_deviation} 小时超过容忍度 {tolerance}",
            'sim_period': [sim_backlog['backlog_start'], sim_backlog['backlog_end']],
            'actual_period': [actual_backlog['backlog_start'], actual_backlog['backlog_end']],
            'deviation': max_deviation,
            'start_deviation': start_deviation,
            'end_deviation': end_deviation
        }
    
    def calculate_metric_2_duration_consistency(self, sim_backlog: Dict, actual_backlog: Dict) -> Dict:
        """
        指标2: 推演的积压时段持续时长与实际一致
        
        Args:
            sim_backlog: 仿真积压时段数据
            actual_backlog: 实际积压时段数据
            
        Returns:
            dict: 指标2计算结果
        """
        if not actual_backlog or actual_backlog['backlog_duration'] == 0:
            return {
                'passed': False,
                'reason': '无实际积压持续时长数据',
                'sim_duration': sim_backlog['backlog_duration'] if sim_backlog else 0,
                'actual_duration': 0
            }
        
        if not sim_backlog:
            return {
                'passed': False,
                'reason': '仿真未发现积压',
                'sim_duration': 0,
                'actual_duration': actual_backlog['backlog_duration']
            }
        
        sim_duration = sim_backlog['backlog_duration']
        actual_duration = actual_backlog['backlog_duration']
        
        passed = sim_duration == actual_duration
        
        return {
            'passed': passed,
            'reason': f"持续时长一致 ({sim_duration}小时)" if passed else f"仿真 {sim_duration}小时 vs 实际 {actual_duration}小时",
            'sim_duration': sim_duration,
            'actual_duration': actual_duration,
            'duration_difference': sim_duration - actual_duration
        }
    
    def calculate_metric_3_peak_deviation(self, sim_backlog: Dict, actual_backlog: Dict) -> Dict:
        """
        指标3: 积压最高峰航班积压量与实际偏差不高于15%
        
        Args:
            sim_backlog: 仿真积压时段数据
            actual_backlog: 实际积压时段数据
            
        Returns:
            dict: 指标3计算结果
        """
        if not actual_backlog or actual_backlog['max_hourly_backlog'] == 0:
            return {
                'passed': False,
                'reason': '无实际最高峰积压数据',
                'sim_peak': sim_backlog['max_hourly_backlog'] if sim_backlog else 0,
                'actual_peak': 0,
                'deviation_rate': None
            }
        
        if not sim_backlog or sim_backlog['max_hourly_backlog'] == 0:
            return {
                'passed': False,
                'reason': '仿真未发现积压最高峰',
                'sim_peak': 0,
                'actual_peak': actual_backlog['max_hourly_backlog'],
                'deviation_rate': None
            }
        
        sim_peak = sim_backlog['max_hourly_backlog']
        actual_peak = actual_backlog['max_hourly_backlog']
        
        deviation_rate = abs(sim_peak - actual_peak) / actual_peak
        threshold = self.config.validation_parameters['peak_deviation_threshold']
        
        passed = deviation_rate <= threshold
        
        return {
            'passed': passed,
            'reason': f"偏差率 {deviation_rate:.1%} 在阈值内" if passed else f"偏差率 {deviation_rate:.1%} 超过阈值 {threshold:.1%}",
            'sim_peak': sim_peak,
            'actual_peak': actual_peak,
            'deviation_rate': deviation_rate,
            'absolute_deviation': abs(sim_peak - actual_peak)
        }
    
    def calculate_metric_4_latest_operation(self, sim_results: List[Dict], actual_data: pd.DataFrame) -> Dict:
        """
        指标4: 推演的航班最晚运行时段与实际一致
        
        Args:
            sim_results: 仿真结果
            actual_data: 实际数据
            
        Returns:
            dict: 指标4计算结果
        """
        # 计算仿真最晚运行时段
        sim_latest_hour = None
        if sim_results:
            latest_times = []
            for result in sim_results:
                if 'actual_takeoff' in result and result['actual_takeoff']:
                    latest_times.append(result['actual_takeoff'])
                elif 'actual_landing' in result and result['actual_landing']:
                    latest_times.append(result['actual_landing'])
            
            if latest_times:
                sim_latest_hour = max(latest_times).hour
        
        # 计算实际最晚运行时段
        actual_latest_hour = None
        if not actual_data.empty:
            time_cols = ['实际起飞', '实际离港', '实际降落', '实际到达']
            latest_times = []
            
            for col in time_cols:
                if col in actual_data.columns:
                    times = pd.to_datetime(actual_data[col], errors='coerce').dropna()
                    if not times.empty:
                        latest_times.extend(times.tolist())
            
            if latest_times:
                actual_latest_hour = max(latest_times).hour
        
        # 判断一致性
        if actual_latest_hour is None:
            return {
                'passed': False,
                'reason': '无实际最晚运行时段数据',
                'sim_latest_hour': sim_latest_hour,
                'actual_latest_hour': None
            }
        
        if sim_latest_hour is None:
            return {
                'passed': False,
                'reason': '仿真无最晚运行时段',
                'sim_latest_hour': None,
                'actual_latest_hour': actual_latest_hour
            }
        
        passed = sim_latest_hour == actual_latest_hour
        
        return {
            'passed': passed,
            'reason': f"最晚运行时段一致 ({sim_latest_hour}点)" if passed else f"仿真 {sim_latest_hour}点 vs 实际 {actual_latest_hour}点",
            'sim_latest_hour': sim_latest_hour,
            'actual_latest_hour': actual_latest_hour,
            'hour_difference': abs(sim_latest_hour - actual_latest_hour) if sim_latest_hour and actual_latest_hour else None
        }
    
    def calculate_all_metrics(self, simulation_results: Dict, actual_data: pd.DataFrame) -> Dict:
        """
        计算所有四项验证指标
        
        Args:
            simulation_results: 仿真结果
            actual_data: 实际数据
            
        Returns:
            dict: 所有指标计算结果
        """
        print("正在计算验证指标...")
        
        # 合并出港和入港结果
        all_sim_results = simulation_results.get('departures', []) + simulation_results.get('arrivals', [])
        
        # 计算积压时段
        backlog_analysis = self.calculate_backlog_periods(all_sim_results, actual_data)
        sim_backlog = backlog_analysis['simulation']
        actual_backlog = backlog_analysis['actual']
        
        # 计算四项指标
        metrics = {
            'metric_1_period_deviation': self.calculate_metric_1_period_deviation(sim_backlog, actual_backlog),
            'metric_2_duration_consistency': self.calculate_metric_2_duration_consistency(sim_backlog, actual_backlog),
            'metric_3_peak_deviation': self.calculate_metric_3_peak_deviation(sim_backlog, actual_backlog),
            'metric_4_latest_operation': self.calculate_metric_4_latest_operation(all_sim_results, actual_data)
        }
        
        # 计算总体通过情况
        passed_count = sum(1 for metric in metrics.values() if metric['passed'])
        total_count = len(metrics)
        
        overall_result = {
            'all_metrics': metrics,
            'summary': {
                'passed_count': passed_count,
                'total_count': total_count,
                'pass_rate': passed_count / total_count,
                'overall_passed': passed_count == total_count
            },
            'backlog_analysis': backlog_analysis
        }
        
        return overall_result
    
    def print_metrics_report(self, metrics_result: Dict):
        """打印指标验证报告"""
        print("\n=== 四项验证指标结果 ===")
        
        metrics = metrics_result['all_metrics']
        
        print("指标1 - 积压时段偏差:")
        metric1 = metrics['metric_1_period_deviation']
        print(f"  结果: {'✓ 通过' if metric1['passed'] else '✗ 未通过'}")
        print(f"  说明: {metric1['reason']}")
        if metric1.get('sim_period') and metric1.get('actual_period'):
            print(f"  仿真时段: {metric1['sim_period'][0]}-{metric1['sim_period'][1]}点")
            print(f"  实际时段: {metric1['actual_period'][0]}-{metric1['actual_period'][1]}点")
        
        print("\n指标2 - 持续时长一致性:")
        metric2 = metrics['metric_2_duration_consistency']
        print(f"  结果: {'✓ 通过' if metric2['passed'] else '✗ 未通过'}")
        print(f"  说明: {metric2['reason']}")
        
        print("\n指标3 - 最高峰偏差:")
        metric3 = metrics['metric_3_peak_deviation']
        print(f"  结果: {'✓ 通过' if metric3['passed'] else '✗ 未通过'}")
        print(f"  说明: {metric3['reason']}")
        if metric3.get('sim_peak') is not None and metric3.get('actual_peak') is not None:
            print(f"  仿真最高峰: {metric3['sim_peak']} 架次/小时")
            print(f"  实际最高峰: {metric3['actual_peak']} 架次/小时")
        
        print("\n指标4 - 最晚运行时段:")
        metric4 = metrics['metric_4_latest_operation']
        print(f"  结果: {'✓ 通过' if metric4['passed'] else '✗ 未通过'}")
        print(f"  说明: {metric4['reason']}")
        
        # 总体结果
        summary = metrics_result['summary']
        print(f"\n=== 总体验证结果 ===")
        print(f"通过指标: {summary['passed_count']}/{summary['total_count']}")
        print(f"通过率: {summary['pass_rate']:.1%}")
        print(f"整体评价: {'✓ 仿真验证通过' if summary['overall_passed'] else '✗ 仿真需要调优'}")

# 创建指标计算器
def create_metrics_calculator(config) -> MetricsCalculator:
    """创建指标计算器实例"""
    return MetricsCalculator(config)
