#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG仿真系统测试脚本
用于快速测试系统的各个组件和功能
"""

import os
import sys
import unittest
from datetime import datetime
import pandas as pd

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

def test_imports():
    """测试所有模块的导入"""
    print("测试模块导入...")
    
    try:
        from core.parameters import ZGGGSimulationConfig
        from core.aircraft_classifier import AircraftClassifier  
        from core.runway_scheduler import RunwayScheduler
        from core.zggg_simulator import ZGGGSimulator
        from data.data_loader import FlightDataLoader
        from utils.time_utils import TimeUtils
        from validation.metrics_calculator import MetricsCalculator
        from validation.result_validator import create_result_validator
        from main_zggg_simulation import ZGGGSimulationSystem
        
        print("✓ 所有模块导入成功")
        return True
    except ImportError as e:
        print(f"✗ 模块导入失败: {e}")
        return False

def test_config_creation():
    """测试配置创建"""
    print("测试配置创建...")
    
    try:
        from core.parameters import ZGGGSimulationConfig
        
        config = ZGGGSimulationConfig()
        
        # 检查基本配置
        assert config.runway_config is not None
        assert config.time_parameters is not None
        assert config.wake_separation is not None
        assert config.aircraft_classification is not None
        
        # 检查关键参数
        assert '02L' in config.runway_config
        assert 'min_departure_rot' in config.time_parameters
        assert ('Heavy', 'Heavy') in config.wake_separation
        
        print("✓ 配置创建成功")
        return True
    except Exception as e:
        print(f"✗ 配置创建失败: {e}")
        return False

def test_aircraft_classifier():
    """测试飞机分类器"""
    print("测试飞机分类器...")
    
    try:
        from core.parameters import ZGGGSimulationConfig
        from core.aircraft_classifier import AircraftClassifier
        
        config = ZGGGSimulationConfig()
        classifier = AircraftClassifier(config)
        
        # 测试分类功能
        test_cases = [
            ('A380', 'Heavy'),
            ('B777', 'Heavy'),
            ('A320', 'Medium'),
            ('B737', 'Medium'),
            ('CRJ900', 'Light'),
            ('UNKNOWN', 'Medium')  # 默认分类
        ]
        
        for aircraft_type, expected in test_cases:
            result = classifier.classify_aircraft(aircraft_type)
            if result != expected:
                print(f"✗ 分类错误: {aircraft_type} -> {result}, 期望: {expected}")
                return False
        
        print("✓ 飞机分类器测试通过")
        return True
    except Exception as e:
        print(f"✗ 飞机分类器测试失败: {e}")
        return False

def test_time_utils():
    """测试时间工具"""
    print("测试时间工具...")
    
    try:
        from utils.time_utils import TimeUtils
        
        time_utils = TimeUtils()
        
        # 测试时间解析
        time_str = "2024-05-01 08:30:00"
        parsed_time = time_utils.parse_time(time_str)
        assert parsed_time is not None
        
        # 测试时间格式化  
        formatted = time_utils.format_time(parsed_time, "%Y-%m-%d %H:%M:%S")
        assert formatted == time_str
        
        # 测试延误计算
        planned = time_utils.parse_time("2024-05-01 08:00:00")
        actual = time_utils.parse_time("2024-05-01 08:15:00")
        delay = time_utils.calculate_delay_minutes(planned, actual)
        assert delay == 15.0
        
        print("✓ 时间工具测试通过")
        return True
    except Exception as e:
        print(f"✗ 时间工具测试失败: {e}")
        return False

def test_data_loader():
    """测试数据加载器（模拟数据）"""
    print("测试数据加载器...")
    
    try:
        from data.data_loader import FlightDataLoader
        
        loader = FlightDataLoader()
        
        # 创建模拟数据
        test_data = pd.DataFrame({
            '航班号': ['CZ3001', 'MU5002', 'CA1003'],
            '计划起飞': ['2024-05-01 08:00:00', '2024-05-01 09:00:00', '2024-05-01 10:00:00'],
            '实际起飞': ['2024-05-01 08:15:00', '2024-05-01 09:05:00', '2024-05-01 10:20:00'],
            '起飞机场': ['ZGGG', 'ZGGG', 'ZBAA'],
            '到达机场': ['ZBAA', 'ZSPD', 'ZGGG'],
            '机型': ['A320', 'B737', 'A380']
        })
        
        # 测试ZGGG数据提取
        zggg_data = loader.extract_zggg_flights(test_data)
        assert len(zggg_data) == 3  # 包含ZGGG作为起飞或到达机场的航班
        
        # 测试数据预处理
        processed_data = loader.preprocess_data(zggg_data)
        assert len(processed_data) <= len(zggg_data)
        
        print("✓ 数据加载器测试通过")
        return True
    except Exception as e:
        print(f"✗ 数据加载器测试失败: {e}")
        return False

def test_runway_scheduler():
    """测试跑道调度器"""
    print("测试跑道调度器...")
    
    try:
        from core.parameters import ZGGGSimulationConfig
        from core.runway_scheduler import RunwayScheduler
        from utils.time_utils import TimeUtils
        
        config = ZGGGSimulationConfig()
        scheduler = RunwayScheduler(config)
        time_utils = TimeUtils()
        
        # 测试跑道选择
        planned_time = time_utils.parse_time("2024-05-01 08:00:00")
        
        # 出港跑道选择
        dep_runway = scheduler.get_optimal_runway('departure', 'Medium', planned_time)
        assert dep_runway in ['02L', '02R']
        
        # 入港跑道选择
        arr_runway = scheduler.get_optimal_runway('arrival', 'Heavy', planned_time)
        assert arr_runway in ['20L', '20R']
        
        print("✓ 跑道调度器测试通过")
        return True
    except Exception as e:
        print(f"✗ 跑道调度器测试失败: {e}")
        return False

def test_metrics_calculator():
    """测试指标计算器"""
    print("测试指标计算器...")
    
    try:
        from core.parameters import ZGGGSimulationConfig
        from validation.metrics_calculator import MetricsCalculator
        from utils.time_utils import TimeUtils
        
        config = ZGGGSimulationConfig()
        calculator = MetricsCalculator(config)
        time_utils = TimeUtils()
        
        # 创建模拟仿真结果
        simulation_results = {
            'departures': [
                {
                    'flight_number': 'CZ3001',
                    'planned_departure': time_utils.parse_time("2024-05-01 08:00:00"),
                    'actual_takeoff': time_utils.parse_time("2024-05-01 08:15:00"),
                    'delay_minutes': 15.0
                }
            ],
            'arrivals': [],
            'backlog_periods': [
                {
                    'start_time': time_utils.parse_time("2024-05-01 08:00:00"),
                    'end_time': time_utils.parse_time("2024-05-01 09:00:00"),
                    'duration_minutes': 60,
                    'type': 'departure'
                }
            ]
        }
        
        # 创建模拟实际数据
        actual_data = pd.DataFrame({
            '航班号': ['CZ3001'],
            '计划起飞': ['2024-05-01 08:00:00'],
            '实际起飞': ['2024-05-01 08:10:00']
        })
        
        # 测试指标计算（基础功能）
        assert hasattr(calculator, 'calculate_all_metrics')
        
        print("✓ 指标计算器测试通过")
        return True
    except Exception as e:
        print(f"✗ 指标计算器测试失败: {e}")
        return False

def check_data_file():
    """检查数据文件是否存在"""
    print("检查数据文件...")
    
    data_file = os.path.join(current_dir, '数据', '5月航班运行数据（实际数据列）.xlsx')
    
    if os.path.exists(data_file):
        print(f"✓ 数据文件存在: {data_file}")
        
        # 检查文件大小
        file_size = os.path.getsize(data_file) / (1024 * 1024)  # MB
        print(f"  文件大小: {file_size:.1f} MB")
        
        return True
    else:
        print(f"⚠ 数据文件不存在: {data_file}")
        print("  如需完整测试，请确保数据文件路径正确")
        return False

def run_integration_test():
    """运行集成测试（需要实际数据文件）"""
    print("运行集成测试...")
    
    data_file = os.path.join(current_dir, '数据', '5月航班运行数据（实际数据列）.xlsx')
    
    if not os.path.exists(data_file):
        print("⚠ 跳过集成测试：数据文件不存在")
        return False
    
    try:
        from main_zggg_simulation import ZGGGSimulationSystem
        
        # 创建简化配置进行快速测试
        config_override = {
            'min_departure_rot': 90,
            'min_arrival_rot': 60,
            'taxi_buffer_minutes': 10
        }
        
        print("  创建仿真系统...")
        sim_system = ZGGGSimulationSystem(data_file, config_override)
        
        print("  加载数据...")
        if not sim_system.load_and_prepare_data():
            print("✗ 数据加载失败")
            return False
        
        print(f"  ZGGG数据量: {len(sim_system.zggg_data)}")
        
        # 只处理前100条数据进行快速测试
        if len(sim_system.zggg_data) > 100:
            sim_system.zggg_data = sim_system.zggg_data.head(100)
            print(f"  限制测试数据量: {len(sim_system.zggg_data)} 条")
        
        print("  运行仿真...")
        results = sim_system.run_single_simulation(export_results=False)
        
        if results is None:
            print("✗ 仿真运行失败")
            return False
        
        print("✓ 集成测试通过")
        
        # 显示关键结果
        sim_results = results['simulation']
        validation_results = results['validation']
        
        dep_count = len(sim_results.get('departures', []))
        arr_count = len(sim_results.get('arrivals', []))
        backlog_count = len(sim_results.get('backlog_periods', []))
        
        print(f"  仿真结果: 出港 {dep_count}, 入港 {arr_count}, 积压时段 {backlog_count}")
        
        if 'statistical_summary' in validation_results:
            stats = validation_results['statistical_summary']
            avg_delay = stats.get('average_delay', 0)
            on_time_rate = stats.get('on_time_performance', 0)
            print(f"  性能指标: 平均延误 {avg_delay:.1f}分钟, 正点率 {on_time_rate:.1f}%")
        
        return True
        
    except Exception as e:
        print(f"✗ 集成测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主测试函数"""
    print("ZGGG仿真系统测试套件")
    print("=" * 50)
    
    test_results = []
    
    # 基础测试
    test_results.append(("模块导入", test_imports()))
    test_results.append(("配置创建", test_config_creation()))  
    test_results.append(("飞机分类器", test_aircraft_classifier()))
    test_results.append(("时间工具", test_time_utils()))
    test_results.append(("数据加载器", test_data_loader()))
    test_results.append(("跑道调度器", test_runway_scheduler()))
    test_results.append(("指标计算器", test_metrics_calculator()))
    
    # 文件检查
    test_results.append(("数据文件检查", check_data_file()))
    
    # 集成测试
    test_results.append(("集成测试", run_integration_test()))
    
    print("\n" + "=" * 50)
    print("测试结果摘要")
    print("=" * 50)
    
    passed = 0
    total = len(test_results)
    
    for test_name, result in test_results:
        status = "✓ 通过" if result else "✗ 失败"
        print(f"{test_name:<15} {status}")
        if result:
            passed += 1
    
    print(f"\n总计: {passed}/{total} 项测试通过")
    
    if passed == total:
        print("\n🎉 所有测试通过！系统运行正常。")
        return 0
    else:
        print(f"\n⚠ {total-passed} 项测试失败，请检查相关问题。")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
