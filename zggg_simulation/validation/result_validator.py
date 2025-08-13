#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
结果验证器 - 验证仿真结果与实际数据的一致性
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import matplotlib.pyplot as plt
# import seaborn as sns  # 暂时注释掉以避免导入错误

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from validation.metrics_calculator import MetricsCalculator

class ZGGGResultValidator:
    """ZGGG仿真结果验证器"""
    
    def __init__(self, config, actual_data: pd.DataFrame = None):
        """
        初始化验证器
        
        Args:
            config: 仿真配置对象
            actual_data: 实际航班数据
        """
        self.config = config
        self.actual_data = actual_data
        self.metrics_calculator = MetricsCalculator(config)
        
        # 验证结果存储
        self.validation_results = {}
    
    def set_actual_data(self, actual_data: pd.DataFrame):
        """设置实际数据"""
        self.actual_data = actual_data
        print(f"已加载实际数据: {len(actual_data)} 条记录")
    
    def validate_simulation_results(self, simulation_results: Dict) -> Dict:
        """
        验证仿真结果
        
        Args:
            simulation_results: 仿真结果字典
            
        Returns:
            dict: 完整验证结果
        """
        if self.actual_data is None:
            print("警告: 没有实际数据，无法进行完整验证")
            return self._basic_validation(simulation_results)
        
        print("开始全面验证仿真结果...")
        
        # 计算四项核心指标
        metrics_result = self.metrics_calculator.calculate_all_metrics(
            simulation_results, self.actual_data
        )
        
        # 计算其他验证指标
        additional_metrics = self._calculate_additional_metrics(
            simulation_results, self.actual_data
        )
        
        # 整合验证结果
        validation_result = {
            'core_metrics': metrics_result,
            'additional_metrics': additional_metrics,
            'data_comparison': self._compare_data_distributions(simulation_results),
            'validation_timestamp': datetime.now(),
            'recommendation': self._generate_optimization_recommendations(metrics_result)
        }
        
        self.validation_results = validation_result
        return validation_result
    
    def _basic_validation(self, simulation_results: Dict) -> Dict:
        """
        基础验证（无实际数据对比）
        
        Args:
            simulation_results: 仿真结果
            
        Returns:
            dict: 基础验证结果
        """
        print("执行基础验证...")
        
        basic_validation = {
            'data_integrity': self._check_data_integrity(simulation_results),
            'logical_consistency': self._check_logical_consistency(simulation_results),
            'statistical_summary': self._generate_statistical_summary(simulation_results),
            'validation_timestamp': datetime.now()
        }
        
        return basic_validation
    
    def _check_data_integrity(self, simulation_results: Dict) -> Dict:
        """检查数据完整性"""
        departures = simulation_results.get('departures', [])
        arrivals = simulation_results.get('arrivals', [])
        
        integrity_checks = {
            'departure_count': len(departures),
            'arrival_count': len(arrivals),
            'missing_departure_times': 0,
            'missing_arrival_times': 0,
            'invalid_delays': 0
        }
        
        # 检查出港数据完整性
        for dep in departures:
            if not dep.get('actual_takeoff'):
                integrity_checks['missing_departure_times'] += 1
            if dep.get('delay_minutes', 0) < 0:
                integrity_checks['invalid_delays'] += 1
        
        # 检查入港数据完整性
        for arr in arrivals:
            if not arr.get('actual_landing'):
                integrity_checks['missing_arrival_times'] += 1
            if arr.get('delay_minutes', 0) < 0:
                integrity_checks['invalid_delays'] += 1
        
        integrity_checks['integrity_score'] = 1.0 - (
            integrity_checks['missing_departure_times'] + 
            integrity_checks['missing_arrival_times'] + 
            integrity_checks['invalid_delays']
        ) / max(1, len(departures) + len(arrivals))
        
        return integrity_checks
    
    def _check_logical_consistency(self, simulation_results: Dict) -> Dict:
        """检查逻辑一致性"""
        departures = simulation_results.get('departures', [])
        arrivals = simulation_results.get('arrivals', [])
        
        consistency_checks = {
            'time_sequence_violations': 0,
            'extreme_delays': 0,
            'runway_conflicts': 0
        }
        
        # 检查时间序列逻辑
        for dep in departures:
            planned_dep = dep.get('planned_departure')
            actual_takeoff = dep.get('actual_takeoff')
            
            if planned_dep and actual_takeoff:
                if actual_takeoff < planned_dep:
                    consistency_checks['time_sequence_violations'] += 1
                
                delay = dep.get('delay_minutes', 0)
                if delay > 180:  # 超过3小时的极端延误
                    consistency_checks['extreme_delays'] += 1
        
        # 检查跑道冲突（同一时间同一跑道）
        runway_usage = {}
        all_operations = departures + arrivals
        
        for op in all_operations:
            runway = op.get('runway_used')
            time_key = op.get('actual_takeoff') or op.get('actual_landing')
            
            if runway and time_key:
                time_slot = time_key.replace(second=0, microsecond=0)  # 精确到分钟
                if (runway, time_slot) in runway_usage:
                    consistency_checks['runway_conflicts'] += 1
                runway_usage[(runway, time_slot)] = op
        
        total_operations = len(all_operations)
        consistency_checks['consistency_score'] = 1.0 - (
            consistency_checks['time_sequence_violations'] + 
            consistency_checks['extreme_delays'] + 
            consistency_checks['runway_conflicts']
        ) / max(1, total_operations)
        
        return consistency_checks
    
    def _generate_statistical_summary(self, simulation_results: Dict) -> Dict:
        """生成统计摘要"""
        departures = simulation_results.get('departures', [])
        arrivals = simulation_results.get('arrivals', [])
        all_operations = departures + arrivals
        
        if not all_operations:
            return {}
        
        delays = [op.get('delay_minutes', 0) for op in all_operations]
        
        summary = {
            'total_operations': len(all_operations),
            'departure_operations': len(departures),
            'arrival_operations': len(arrivals),
            'average_delay': np.mean(delays),
            'median_delay': np.median(delays),
            'max_delay': np.max(delays),
            'delay_std': np.std(delays),
            'delayed_flights_count': sum(1 for d in delays if d > 15),
            'delayed_flights_percentage': sum(1 for d in delays if d > 15) / len(delays) * 100,
            'on_time_performance': sum(1 for d in delays if d <= 15) / len(delays) * 100
        }
        
        # 跑道利用率统计
        runway_usage = {}
        for op in all_operations:
            runway = op.get('runway_used', 'Unknown')
            runway_usage[runway] = runway_usage.get(runway, 0) + 1
        
        summary['runway_utilization'] = runway_usage
        
        return summary
    
    def _calculate_additional_metrics(self, simulation_results: Dict, actual_data: pd.DataFrame) -> Dict:
        """计算额外的验证指标"""
        additional_metrics = {}
        
        # 延误分布对比
        sim_delays = []
        for op in simulation_results.get('departures', []) + simulation_results.get('arrivals', []):
            sim_delays.append(op.get('delay_minutes', 0))
        
        # 从实际数据计算延误
        actual_delays = self._extract_actual_delays(actual_data)
        
        if sim_delays and actual_delays:
            additional_metrics['delay_distribution_comparison'] = {
                'sim_mean_delay': np.mean(sim_delays),
                'actual_mean_delay': np.mean(actual_delays),
                'mean_delay_difference': np.mean(sim_delays) - np.mean(actual_delays),
                'sim_std_delay': np.std(sim_delays),
                'actual_std_delay': np.std(actual_delays),
                'correlation_coefficient': np.corrcoef(
                    sim_delays[:min(len(sim_delays), len(actual_delays))],
                    actual_delays[:min(len(sim_delays), len(actual_delays))]
                )[0, 1] if len(sim_delays) > 1 and len(actual_delays) > 1 else 0
            }
        
        # 运营量对比
        additional_metrics['operation_volume_comparison'] = self._compare_operation_volumes(
            simulation_results, actual_data
        )
        
        return additional_metrics
    
    def _extract_actual_delays(self, actual_data: pd.DataFrame) -> List[float]:
        """从实际数据提取延误信息"""
        delays = []
        
        time_pairs = [
            ('实际起飞', '计划起飞'),
            ('实际离港', '计划离港'),
            ('实际降落', '计划降落'),
            ('实际到达', '计划到达')
        ]
        
        for _, row in actual_data.iterrows():
            for actual_col, planned_col in time_pairs:
                if actual_col in row and planned_col in row:
                    if pd.notna(row[actual_col]) and pd.notna(row[planned_col]):
                        actual_time = pd.to_datetime(row[actual_col])
                        planned_time = pd.to_datetime(row[planned_col])
                        delay_minutes = (actual_time - planned_time).total_seconds() / 60
                        delays.append(max(0, delay_minutes))  # 只考虑正延误
                        break
        
        return delays
    
    def _compare_operation_volumes(self, simulation_results: Dict, actual_data: pd.DataFrame) -> Dict:
        """对比运营量"""
        sim_departures = len(simulation_results.get('departures', []))
        sim_arrivals = len(simulation_results.get('arrivals', []))
        
        # 从实际数据统计
        actual_departures = 0
        actual_arrivals = 0
        
        departure_cols = ['起飞站', '出发机场']
        arrival_cols = ['到达站', '到达机场']
        
        for col in departure_cols:
            if col in actual_data.columns:
                actual_departures = len(actual_data[actual_data[col].str.contains('ZGGG', na=False)])
                break
        
        for col in arrival_cols:
            if col in actual_data.columns:
                actual_arrivals = len(actual_data[actual_data[col].str.contains('ZGGG', na=False)])
                break
        
        return {
            'sim_departures': sim_departures,
            'actual_departures': actual_departures,
            'departure_difference': sim_departures - actual_departures,
            'sim_arrivals': sim_arrivals,
            'actual_arrivals': actual_arrivals,
            'arrival_difference': sim_arrivals - actual_arrivals,
            'total_sim_operations': sim_departures + sim_arrivals,
            'total_actual_operations': actual_departures + actual_arrivals
        }
    
    def _compare_data_distributions(self, simulation_results: Dict) -> Dict:
        """对比数据分布特征"""
        departures = simulation_results.get('departures', [])
        arrivals = simulation_results.get('arrivals', [])
        
        # 按小时分布
        hourly_departures = {}
        hourly_arrivals = {}
        
        for dep in departures:
            if dep.get('actual_takeoff'):
                hour = dep['actual_takeoff'].hour
                hourly_departures[hour] = hourly_departures.get(hour, 0) + 1
        
        for arr in arrivals:
            if arr.get('actual_landing'):
                hour = arr['actual_landing'].hour
                hourly_arrivals[hour] = hourly_arrivals.get(hour, 0) + 1
        
        return {
            'hourly_departure_distribution': hourly_departures,
            'hourly_arrival_distribution': hourly_arrivals,
            'peak_departure_hour': max(hourly_departures.keys(), key=hourly_departures.get) if hourly_departures else None,
            'peak_arrival_hour': max(hourly_arrivals.keys(), key=hourly_arrivals.get) if hourly_arrivals else None
        }
    
    def _generate_optimization_recommendations(self, metrics_result: Dict) -> List[str]:
        """生成优化建议"""
        recommendations = []
        
        metrics = metrics_result['all_metrics']
        summary = metrics_result['summary']
        
        if not summary['overall_passed']:
            recommendations.append("建议调优仿真参数以提高验证通过率")
            
            # 具体建议
            if not metrics['metric_1_period_deviation']['passed']:
                recommendations.append("调整ROT或尾流间隔参数以改善积压时段预测")
            
            if not metrics['metric_2_duration_consistency']['passed']:
                recommendations.append("重新校准滑行时间或跑道容量参数")
            
            if not metrics['metric_3_peak_deviation']['passed']:
                recommendations.append("优化高峰时段的跑道调度策略")
            
            if not metrics['metric_4_latest_operation']['passed']:
                recommendations.append("调整夜间运营时段的仿真逻辑")
        
        else:
            recommendations.append("仿真验证通过，参数设置合理")
            recommendations.append("可以使用当前参数进行进一步分析")
        
        return recommendations
    
    def print_validation_report(self, validation_result: Dict = None):
        """打印验证报告"""
        if validation_result is None:
            validation_result = self.validation_results
        
        if not validation_result:
            print("没有可用的验证结果")
            return
        
        print("\n" + "="*50)
        print("ZGGG机场仿真验证报告")
        print("="*50)
        
        # 打印核心指标
        if 'core_metrics' in validation_result:
            self.metrics_calculator.print_metrics_report(validation_result['core_metrics'])
        
        # 打印数据完整性
        if 'data_integrity' in validation_result:
            integrity = validation_result['data_integrity']
            print(f"\n=== 数据完整性检查 ===")
            print(f"完整性评分: {integrity['integrity_score']:.3f}")
            print(f"缺失起飞时间: {integrity['missing_departure_times']}")
            print(f"缺失降落时间: {integrity['missing_arrival_times']}")
        
        # 打印统计摘要
        if 'statistical_summary' in validation_result:
            stats = validation_result['statistical_summary']
            print(f"\n=== 仿真统计摘要 ===")
            print(f"总运营量: {stats.get('total_operations', 0)}")
            print(f"平均延误: {stats.get('average_delay', 0):.1f} 分钟")
            print(f"正点率: {stats.get('on_time_performance', 0):.1f}%")
        
        # 打印优化建议
        if 'recommendation' in validation_result:
            print(f"\n=== 优化建议 ===")
            for i, rec in enumerate(validation_result['recommendation'], 1):
                print(f"{i}. {rec}")
    
    def export_validation_report(self, filename: str = "zggg_validation_report.xlsx"):
        """导出验证报告到Excel"""
        if not self.validation_results:
            print("没有可用的验证结果")
            return
        
        print(f"正在导出验证报告到 {filename}...")
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # 导出核心指标
            if 'core_metrics' in self.validation_results:
                core_metrics = self.validation_results['core_metrics']['all_metrics']
                metrics_data = []
                
                for metric_name, metric_data in core_metrics.items():
                    metrics_data.append({
                        '指标': metric_name,
                        '通过状态': '通过' if metric_data['passed'] else '未通过',
                        '说明': metric_data['reason']
                    })
                
                metrics_df = pd.DataFrame(metrics_data)
                metrics_df.to_excel(writer, sheet_name='核心指标', index=False)
            
            # 导出统计摘要
            if 'statistical_summary' in self.validation_results:
                stats_data = []
                for key, value in self.validation_results['statistical_summary'].items():
                    if not isinstance(value, dict):
                        stats_data.append({'指标': key, '数值': value})
                
                stats_df = pd.DataFrame(stats_data)
                stats_df.to_excel(writer, sheet_name='统计摘要', index=False)
        
        print(f"验证报告已导出到 {filename}")

# 创建验证器实例
def create_result_validator(config, actual_data: pd.DataFrame = None) -> ZGGGResultValidator:
    """创建结果验证器实例"""
    return ZGGGResultValidator(config, actual_data)
