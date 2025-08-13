#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG机场仿真系统
用于ZGGG机场的航班仿真、跑道调度优化和延误分析
"""

__version__ = "1.0.0"
__author__ = "ZGGG Simulation Team"
__description__ = "广州白云国际机场仿真系统"

# 导入核心模块
from .core.parameters import ZGGGSimulationConfig
from .core.zggg_simulator import ZGGGSimulator
from .core.aircraft_classifier import AircraftClassifier
from .core.runway_scheduler import RunwayScheduler

# 导入数据处理模块
from .data.data_loader import FlightDataLoader

# 导入工具模块
from .utils.time_utils import TimeUtils

# 导入验证模块
from .validation.metrics_calculator import MetricsCalculator
from .validation.result_validator import create_result_validator

# 导入主执行器
from .main_zggg_simulation import ZGGGSimulationSystem

__all__ = [
    # 版本信息
    '__version__',
    '__author__',
    '__description__',
    
    # 核心模块
    'ZGGGSimulationConfig',
    'ZGGGSimulator', 
    'AircraftClassifier',
    'RunwayScheduler',
    
    # 数据模块
    'FlightDataLoader',
    
    # 工具模块
    'TimeUtils',
    
    # 验证模块
    'MetricsCalculator',
    'create_result_validator',
    
    # 主系统
    'ZGGGSimulationSystem'
]

def get_version():
    """获取系统版本"""
    return __version__

def get_system_info():
    """获取系统信息"""
    return {
        'version': __version__,
        'author': __author__, 
        'description': __description__,
        'modules': __all__
    }

# 快速启动函数
def quick_simulation(data_file_path: str, config_override: dict = None):
    """
    快速启动仿真
    
    Args:
        data_file_path: 数据文件路径
        config_override: 配置覆盖参数
        
    Returns:
        仿真结果
    """
    system = ZGGGSimulationSystem(data_file_path, config_override)
    
    if not system.load_and_prepare_data():
        return None
        
    return system.run_single_simulation()

def optimize_parameters(data_file_path: str, max_iterations: int = 10):
    """
    快速参数优化
    
    Args:
        data_file_path: 数据文件路径
        max_iterations: 最大迭代次数
        
    Returns:
        优化结果
    """
    system = ZGGGSimulationSystem(data_file_path)
    
    if not system.load_and_prepare_data():
        return None
        
    return system.run_optimization_loop(max_iterations)

# 系统启动时的信息打印
print(f"ZGGG仿真系统已加载 (版本 {__version__})")
