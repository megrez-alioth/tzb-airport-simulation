#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG机场仿真参数配置
包含所有仿真相关的参数设置
"""

from datetime import timedelta

class ZGGGSimulationConfig:
    """ZGGG机场仿真配置类"""
    
    def __init__(self):
        # === 跑道配置 ===
        self.runway_config = {
            'departure_runways': ['01L', '01R'],  # 固定2条起飞跑道
            'arrival_runways': ['07L', '07R'],    # 固定2条降落跑道
            'max_departure_runways': 2,           # 最大起飞跑道数
            'max_arrival_runways': 2              # 最大降落跑道数
        }
        
        # === 时间参数 ===
        self.time_parameters = {
            # ROT (跑道占用时间) - 秒
            'rot_takeoff': 60,      # 起飞ROT：60秒
            'rot_landing': 45,      # 降落ROT：45秒
            
            # 滑行时间 - 分钟 (从数据统计得出)
            'taxi_out_mean': 18,    # 出港滑行平均时间
            'taxi_in_mean': 18,     # 入港滑行平均时间
            'taxi_out_std': 5,      # 出港滑行时间标准差
            'taxi_in_std': 5,       # 入港滑行时间标准差
            
            # 积压定义阈值
            'delay_threshold': 15,   # 延误超过15分钟算积压
            'backlog_threshold': 10  # 积压航班超过10架算积压时段
        }
        
        # === 尾流间隔参数 (基于ICAO标准) ===
        self.wake_separation = {
            # (前机重量等级, 后机重量等级): 间隔秒数
            ('Heavy', 'Heavy'): 90,
            ('Heavy', 'Medium'): 120,
            ('Heavy', 'Light'): 180,
            ('Medium', 'Heavy'): 60,
            ('Medium', 'Medium'): 60,
            ('Medium', 'Light'): 120,
            ('Light', 'Heavy'): 60,
            ('Light', 'Medium'): 60,
            ('Light', 'Light'): 60
        }
        
        # === 飞机分类规则 ===
        self.aircraft_classification = {
            # 重型机 - 宽体机和大型机
            'heavy_keywords': [
                '380',  # A380
                '777', '77F', '77L', '77W',  # Boeing 777系列
                '747', '74F', '748',          # Boeing 747系列
                '787', '788', '789',          # Boeing 787系列
                '330', '33F', '338', '339',   # A330系列
                '340', '343', '346',          # A340系列
                '350', '359',                 # A350系列
                'MD1',                        # MD-11
                'IL9',                        # IL-96
            ],
            
            # 中型机 - 窄体干线机
            'medium_keywords': [
                '320', '32A', '32B', '32N', '32S',  # A320系列
                '321', '32Q',                        # A321系列
                '319', '31F',                        # A319系列
                '737', '738', '739', '73G', '73H',   # Boeing 737系列
                '757', '75F',                        # Boeing 757系列
                '767', '76F',                        # Boeing 767系列
                'E90', 'E75',                        # Embraer E-Jets
                'CRJ', 'CR9',                        # Bombardier CRJ系列
            ],
            
            # 轻型机 - 支线机和小型机 (其他归为此类)
            'light_default': True
        }
        
        # === 验证指标参数 ===
        self.validation_parameters = {
            'time_tolerance_hours': 1,        # 时段偏差容忍度（小时）
            'peak_deviation_threshold': 0.15, # 最高峰偏差阈值（15%）
            'analysis_time_unit': 'hour'      # 分析时间单位
        }
    
    def update_parameters(self, **kwargs):
        """更新参数配置"""
        for category, params in kwargs.items():
            if hasattr(self, category):
                getattr(self, category).update(params)
    
    def get_wake_separation(self, previous_weight, current_weight):
        """获取尾流间隔时间"""
        return self.wake_separation.get((previous_weight, current_weight), 60)
    
    def copy(self):
        """复制配置对象"""
        import copy
        return copy.deepcopy(self)

# 默认配置实例
DEFAULT_CONFIG = ZGGGSimulationConfig()
