#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG机场仿真主执行脚本
整合所有模块，实现完整的仿真流程和参数优化
"""

import os
import sys
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.parameters import ZGGGSimulationConfig
from core.zggg_simulator import ZGGGAirportSimulator
from validation.result_validator import create_result_validator
from data.data_loader import FlightDataLoader
from utils.time_utils import TimeUtils

class ZGGGSimulationSystem:
    """ZGGG仿真系统主控制器"""
    
    def __init__(self, data_file_path: str, config_override: Dict = None):
        """
        初始化仿真系统
        
        Args:
            data_file_path: 数据文件路径
            config_override: 配置覆盖参数
        """
        self.data_file_path = data_file_path
        self.config = ZGGGSimulationConfig()
        
        # 应用配置覆盖
        if config_override:
            self._apply_config_override(config_override)
        
        # 初始化组件
        self.data_loader = FlightDataLoader(data_file_path)
        self.simulator = ZGGGAirportSimulator(self.config)
        self.time_utils = TimeUtils()
        
        # 加载数据
        self.actual_data = None
        self.zggg_data = None
        
        print("ZGGG仿真系统初始化完成")
    
    def _apply_config_override(self, override: Dict):
        """应用配置覆盖"""
        for key, value in override.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
                print(f"配置覆盖: {key} = {value}")
    
    def load_and_prepare_data(self) -> bool:
        """加载和准备数据"""
        try:
            print(f"\n{'='*50}")
            print("开始加载数据...")
            
            # 加载原始数据
            self.actual_data = self.data_loader.load_raw_data()
            
            if self.actual_data is None or len(self.actual_data) == 0:
                print("错误: 无法加载数据或数据为空")
                return False
            
            print(f"成功加载 {len(self.actual_data)} 条原始航班数据")
            
            # 提取ZGGG相关数据
            self.zggg_data = self.data_loader.extract_zggg_data(self.actual_data)
            
            if self.zggg_data is None or len(self.zggg_data) == 0:
                print("错误: 没有找到ZGGG相关航班数据")
                return False
            
            print(f"成功提取 {len(self.zggg_data)} 条ZGGG航班数据")
            
            # 数据预处理
            processed_data = self.data_loader.preprocess_time_columns(self.zggg_data)
            print(f"数据预处理完成，有效数据 {len(processed_data)} 条")
            
            # 显示预处理后的列
            print(f"预处理后的列: {list(processed_data.columns)}")
            
            self.zggg_data = processed_data
            return True
            
        except Exception as e:
            print(f"数据加载失败: {e}")
            return False
    
    def run_single_simulation(self, export_results: bool = True) -> Optional[Dict]:
        """
        运行单次仿真
        
        Args:
            export_results: 是否导出结果
            
        Returns:
            dict: 仿真结果，失败返回None
        """
        if self.zggg_data is None:
            print("错误: 没有可用的ZGGG数据")
            return None
        
        try:
            print(f"\n{'='*50}")
            print("开始执行仿真...")
            start_time = datetime.now()
            
            # 加载数据到仿真器
            self.simulator.load_flight_data(self.zggg_data)
            
            # 运行仿真
            simulation_results = self.simulator.run_simulation()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print(f"仿真完成，耗时 {duration:.2f} 秒")
            
            # 验证结果
            validator = create_result_validator(self.config, self.actual_data)
            validation_results = validator.validate_simulation_results(simulation_results)
            
            # 打印验证报告
            validator.print_validation_report(validation_results)
            
            # 导出结果
            if export_results:
                self._export_simulation_results(simulation_results, validation_results)
            
            # 组合完整结果
            complete_results = {
                'simulation': simulation_results,
                'validation': validation_results,
                'metadata': {
                    'config': self._config_to_dict(),
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration_seconds': duration,
                    'data_file': self.data_file_path,
                    'zggg_flights_count': len(self.zggg_data)
                }
            }
            
            return complete_results
            
        except Exception as e:
            print(f"仿真执行失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def run_optimization_loop(self, max_iterations: int = 10, target_metrics: Dict = None) -> Dict:
        """
        运行参数优化循环
        
        Args:
            max_iterations: 最大迭代次数
            target_metrics: 目标指标设定
            
        Returns:
            dict: 优化结果
        """
        if target_metrics is None:
            target_metrics = {
                'min_passed_metrics': 3,  # 至少3个指标通过
                'max_mean_delay': 30.0,   # 平均延误不超过30分钟
                'min_on_time_rate': 70.0  # 正点率不低于70%
            }
        
        print(f"\n{'='*60}")
        print("开始参数优化循环...")
        print(f"目标: {target_metrics}")
        print(f"最大迭代次数: {max_iterations}")
        
        optimization_history = []
        best_config = None
        best_score = -1
        
        # 定义参数优化范围
        parameter_ranges = {
            'min_departure_rot': [90, 120, 150, 180],  # 出港ROT
            'min_arrival_rot': [60, 90, 120],          # 入港ROT
            'taxi_buffer_minutes': [5, 10, 15, 20],    # 滑行缓冲时间
        }
        
        iteration = 0
        
        # 尝试不同参数组合
        for dep_rot in parameter_ranges['min_departure_rot']:
            for arr_rot in parameter_ranges['min_arrival_rot']:
                for taxi_buffer in parameter_ranges['taxi_buffer_minutes']:
                    
                    if iteration >= max_iterations:
                        break
                    
                    iteration += 1
                    print(f"\n--- 优化迭代 {iteration} ---")
                    print(f"参数: ROT出港={dep_rot}s, ROT入港={arr_rot}s, 滑行缓冲={taxi_buffer}min")
                    
                    # 更新配置
                    config_override = {
                        'min_departure_rot': dep_rot,
                        'min_arrival_rot': arr_rot,
                        'taxi_buffer_minutes': taxi_buffer
                    }
                    
                    # 创建新的仿真系统实例
                    sim_system = ZGGGSimulationSystem(self.data_file_path, config_override)
                    
                    if not sim_system.load_and_prepare_data():
                        print("数据加载失败，跳过此次迭代")
                        continue
                    
                    # 运行仿真
                    results = sim_system.run_single_simulation(export_results=False)
                    
                    if results is None:
                        print("仿真失败，跳过此次迭代")
                        continue
                    
                    # 评估结果
                    score = self._evaluate_simulation_results(results, target_metrics)
                    
                    print(f"评估得分: {score:.3f}")
                    
                    # 记录历史
                    optimization_history.append({
                        'iteration': iteration,
                        'config': config_override,
                        'score': score,
                        'results': results
                    })
                    
                    # 更新最佳配置
                    if score > best_score:
                        best_score = score
                        best_config = config_override.copy()
                        print(f"发现更优配置! 得分: {best_score:.3f}")
        
        print(f"\n{'='*60}")
        print("参数优化完成!")
        print(f"最佳配置: {best_config}")
        print(f"最佳得分: {best_score:.3f}")
        
        # 使用最佳配置再次运行仿真
        print("\n使用最佳配置运行最终仿真...")
        final_system = ZGGGSimulationSystem(self.data_file_path, best_config)
        final_system.load_and_prepare_data()
        final_results = final_system.run_single_simulation(export_results=True)
        
        return {
            'best_config': best_config,
            'best_score': best_score,
            'optimization_history': optimization_history,
            'final_results': final_results
        }
    
    def _evaluate_simulation_results(self, results: Dict, target_metrics: Dict) -> float:
        """评估仿真结果质量"""
        try:
            validation = results['validation']
            
            score = 0.0
            
            # 核心指标得分 (40%)
            if 'core_metrics' in validation:
                core_metrics = validation['core_metrics']['all_metrics']
                passed_count = sum(1 for m in core_metrics.values() if m['passed'])
                core_score = passed_count / 4.0 * 0.4
                score += core_score
                
                # 额外奖励：所有指标通过
                if passed_count >= target_metrics.get('min_passed_metrics', 3):
                    score += 0.1
            
            # 延误性能得分 (30%)
            if 'statistical_summary' in validation:
                stats = validation['statistical_summary']
                
                mean_delay = stats.get('average_delay', 999)
                target_delay = target_metrics.get('max_mean_delay', 30.0)
                
                if mean_delay <= target_delay:
                    delay_score = 0.3
                else:
                    delay_score = max(0, 0.3 * (1 - (mean_delay - target_delay) / target_delay))
                
                score += delay_score
            
            # 正点率得分 (30%)
            if 'statistical_summary' in validation:
                stats = validation['statistical_summary']
                
                on_time_rate = stats.get('on_time_performance', 0)
                target_rate = target_metrics.get('min_on_time_rate', 70.0)
                
                if on_time_rate >= target_rate:
                    on_time_score = 0.3
                else:
                    on_time_score = max(0, 0.3 * on_time_rate / target_rate)
                
                score += on_time_score
            
            return min(1.0, score)  # 最大得分1.0
            
        except Exception as e:
            print(f"评估结果时出错: {e}")
            return 0.0
    
    def _export_simulation_results(self, simulation_results: Dict, validation_results: Dict):
        """导出仿真结果"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # 导出到Excel
            excel_filename = f"zggg_simulation_results_{timestamp}.xlsx"
            
            print(f"正在导出结果到 {excel_filename}...")
            
            with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
                
                # 导出出港仿真结果
                if simulation_results.get('departures'):
                    dep_data = []
                    for dep in simulation_results['departures']:
                        dep_data.append({
                            '航班号': dep.get('flight_number', ''),
                            '计划起飞': dep.get('planned_departure', ''),
                            '实际起飞': dep.get('actual_takeoff', ''),
                            '延误(分钟)': dep.get('delay_minutes', 0),
                            '跑道': dep.get('runway_used', ''),
                            '机型': dep.get('aircraft_type', ''),
                            '重量类别': dep.get('weight_category', '')
                        })
                    
                    dep_df = pd.DataFrame(dep_data)
                    dep_df.to_excel(writer, sheet_name='出港仿真结果', index=False)
                
                # 导出入港仿真结果
                if simulation_results.get('arrivals'):
                    arr_data = []
                    for arr in simulation_results['arrivals']:
                        arr_data.append({
                            '航班号': arr.get('flight_number', ''),
                            '计划降落': arr.get('planned_arrival', ''),
                            '实际降落': arr.get('actual_landing', ''),
                            '延误(分钟)': arr.get('delay_minutes', 0),
                            '跑道': arr.get('runway_used', ''),
                            '机型': arr.get('aircraft_type', ''),
                            '重量类别': arr.get('weight_category', '')
                        })
                    
                    arr_df = pd.DataFrame(arr_data)
                    arr_df.to_excel(writer, sheet_name='入港仿真结果', index=False)
                
                # 导出积压时段分析
                if simulation_results.get('backlog_periods'):
                    backlog_data = []
                    for period in simulation_results['backlog_periods']:
                        backlog_data.append({
                            '开始时间': period.get('start_time', ''),
                            '结束时间': period.get('end_time', ''),
                            '持续时长(分钟)': period.get('duration_minutes', 0),
                            '积压航班数': period.get('backlog_count', 0),
                            '类型': period.get('type', '')
                        })
                    
                    backlog_df = pd.DataFrame(backlog_data)
                    backlog_df.to_excel(writer, sheet_name='积压时段分析', index=False)
                
                # 导出验证指标
                if validation_results.get('core_metrics'):
                    metrics_data = []
                    core_metrics = validation_results['core_metrics']['all_metrics']
                    for metric_name, metric_data in core_metrics.items():
                        metrics_data.append({
                            '指标名称': metric_name,
                            '通过状态': '通过' if metric_data['passed'] else '未通过',
                            '说明': metric_data['reason']
                        })
                    
                    metrics_df = pd.DataFrame(metrics_data)
                    metrics_df.to_excel(writer, sheet_name='验证指标', index=False)
            
            print(f"结果已导出到 {excel_filename}")
            
            # 导出配置信息到JSON
            config_filename = f"zggg_simulation_config_{timestamp}.json"
            with open(config_filename, 'w', encoding='utf-8') as f:
                json.dump(self._config_to_dict(), f, ensure_ascii=False, indent=2, default=str)
            
            print(f"配置信息已导出到 {config_filename}")
            
        except Exception as e:
            print(f"导出结果时出错: {e}")
    
    def _config_to_dict(self) -> Dict:
        """将配置对象转换为字典"""
        config_dict = {
            'runway_config': self.config.runway_config,
            'time_parameters': self.config.time_parameters,
            'wake_separation': self.config.wake_separation,
            'aircraft_classification': self.config.aircraft_classification
        }
        
        # 安全地添加可选属性
        if hasattr(self.config, 'validation_thresholds'):
            config_dict['validation_thresholds'] = self.config.validation_thresholds
            
        return config_dict
    
    def print_system_info(self):
        """打印系统信息"""
        print(f"\n{'='*60}")
        print("ZGGG机场仿真系统信息")
        print("="*60)
        print(f"数据文件: {self.data_file_path}")
        print(f"ZGGG航班数据: {len(self.zggg_data) if self.zggg_data is not None else '未加载'} 条")
        print(f"仿真跑道配置:")
        
        # 安全地处理runway_config
        if hasattr(self.config, 'runway_config') and isinstance(self.config.runway_config, dict):
            for runway_id, runway_info in self.config.runway_config.items():
                if isinstance(runway_info, dict) and 'type' in runway_info:
                    print(f"  - {runway_id}: {runway_info['type']}")
                else:
                    print(f"  - {runway_id}: {runway_info}")
        else:
            print("  - 跑道配置格式异常")
            
        print(f"时间参数:")
        if hasattr(self.config, 'time_parameters') and isinstance(self.config.time_parameters, dict):
            print(f"  - 出港ROT: {self.config.time_parameters.get('min_departure_rot', '未设置')}秒")
            print(f"  - 入港ROT: {self.config.time_parameters.get('min_arrival_rot', '未设置')}秒") 
            print(f"  - 滑行缓冲: {self.config.time_parameters.get('taxi_buffer_minutes', '未设置')}分钟")
        else:
            print("  - 时间参数格式异常")
        print("="*60)

def main():
    """主函数"""
    # 配置数据文件路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_file = os.path.join(os.path.dirname(current_dir), '数据', '5月航班运行数据（实际数据列）.xlsx')
    
    print("ZGGG机场仿真系统")
    print("="*50)
    
    # 检查数据文件
    if not os.path.exists(data_file):
        print(f"错误: 数据文件不存在 - {data_file}")
        print("请确保数据文件路径正确")
        return
    
    try:
        # 创建仿真系统
        sim_system = ZGGGSimulationSystem(data_file)
        
        # 加载数据
        if not sim_system.load_and_prepare_data():
            print("数据加载失败，退出程序")
            return
        
        # 打印系统信息
        sim_system.print_system_info()
        
        # 用户选择运行模式
        print("\n请选择运行模式:")
        print("1. 单次仿真 (使用默认参数)")
        print("2. 参数优化 (自动寻找最佳参数)")
        print("3. 退出")
        
        choice = input("\n请输入选择 (1-3): ").strip()
        
        if choice == '1':
            print("\n开始单次仿真...")
            results = sim_system.run_single_simulation()
            if results:
                print("\n仿真完成! 结果已导出到Excel文件")
            else:
                print("仿真失败")
        
        elif choice == '2':
            print("\n开始参数优化...")
            max_iter = int(input("请输入最大迭代次数 (建议5-20): ") or "10")
            optimization_results = sim_system.run_optimization_loop(max_iterations=max_iter)
            print(f"\n参数优化完成! 最佳配置: {optimization_results['best_config']}")
        
        elif choice == '3':
            print("程序退出")
        
        else:
            print("无效选择，程序退出")
    
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"\n程序执行出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
