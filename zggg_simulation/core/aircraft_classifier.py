#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞机分类器 - 基于机型代码识别飞机重量等级
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from .parameters import DEFAULT_CONFIG

class AircraftClassifier:
    """飞机重量等级分类器"""
    
    def __init__(self, config=None):
        self.config = config or DEFAULT_CONFIG
        self.classification_rules = self.config.aircraft_classification
        self._classification_cache = {}  # 缓存分类结果
        
    def classify_aircraft(self, aircraft_type: str) -> str:
        """
        根据机型代码分类飞机重量等级
        
        Args:
            aircraft_type: 机型代码 (如 'B738', 'A320', 'B777')
            
        Returns:
            str: 'Heavy', 'Medium', 'Light'
        """
        if not aircraft_type or pd.isna(aircraft_type):
            return 'Light'  # 默认为轻型机
            
        # 检查缓存
        if aircraft_type in self._classification_cache:
            return self._classification_cache[aircraft_type]
            
        # 转换为大写并去除空格
        aircraft_type = str(aircraft_type).upper().strip()
        
        # 检查重型机关键词
        for keyword in self.classification_rules['heavy_keywords']:
            if keyword in aircraft_type:
                self._classification_cache[aircraft_type] = 'Heavy'
                return 'Heavy'
        
        # 检查中型机关键词
        for keyword in self.classification_rules['medium_keywords']:
            if keyword in aircraft_type:
                self._classification_cache[aircraft_type] = 'Medium'
                return 'Medium'
        
        # 默认为轻型机
        self._classification_cache[aircraft_type] = 'Light'
        return 'Light'
    
    def classify_flight_data(self, df: pd.DataFrame, aircraft_column: str = '机型') -> pd.DataFrame:
        """
        为整个航班数据集添加重量等级分类
        
        Args:
            df: 航班数据DataFrame
            aircraft_column: 机型列名
            
        Returns:
            pd.DataFrame: 添加了'weight_class'列的数据
        """
        df = df.copy()
        df['weight_class'] = df[aircraft_column].apply(self.classify_aircraft)
        return df
    
    def get_classification_statistics(self, df: pd.DataFrame) -> Dict:
        """
        获取分类统计信息
        
        Args:
            df: 包含weight_class列的航班数据
            
        Returns:
            dict: 分类统计结果
        """
        if 'weight_class' not in df.columns:
            raise ValueError("DataFrame must contain 'weight_class' column")
            
        stats = {}
        total_flights = len(df)
        
        for weight_class in ['Heavy', 'Medium', 'Light']:
            count = len(df[df['weight_class'] == weight_class])
            percentage = (count / total_flights) * 100 if total_flights > 0 else 0
            stats[weight_class] = {
                'count': count,
                'percentage': percentage
            }
        
        # 获取每类最常见的机型
        for weight_class in ['Heavy', 'Medium', 'Light']:
            class_data = df[df['weight_class'] == weight_class]
            if len(class_data) > 0:
                top_types = class_data['机型'].value_counts().head(5).to_dict()
                stats[weight_class]['top_aircraft_types'] = top_types
            else:
                stats[weight_class]['top_aircraft_types'] = {}
        
        return stats
    
    def validate_classification(self, df: pd.DataFrame) -> Dict:
        """
        验证分类结果的合理性
        
        Args:
            df: 包含weight_class列的航班数据
            
        Returns:
            dict: 验证结果
        """
        stats = self.get_classification_statistics(df)
        
        validation_results = {
            'total_flights': len(df),
            'classification_distribution': stats,
            'validation_checks': {}
        }
        
        # 检查1: 重型机比例应该合理 (一般5-20%)
        heavy_percentage = stats['Heavy']['percentage']
        validation_results['validation_checks']['heavy_ratio_reasonable'] = 5 <= heavy_percentage <= 25
        
        # 检查2: 中型机应该是主力 (一般50-80%)
        medium_percentage = stats['Medium']['percentage']
        validation_results['validation_checks']['medium_ratio_reasonable'] = 40 <= medium_percentage <= 85
        
        # 检查3: 没有未分类的飞机
        unclassified_count = len(df[df['weight_class'].isna()])
        validation_results['validation_checks']['no_unclassified'] = unclassified_count == 0
        
        return validation_results
    
    def print_classification_report(self, df: pd.DataFrame):
        """打印分类报告"""
        validation = self.validate_classification(df)
        
        print("=== ZGGG机场飞机分类报告 ===")
        print(f"总航班数: {validation['total_flights']}")
        print()
        
        print("分类分布:")
        for weight_class, stats in validation['classification_distribution'].items():
            print(f"{weight_class:>6}: {stats['count']:>6} 架次 ({stats['percentage']:>5.1f}%)")
            
            # 显示前3个最常见机型
            if stats['top_aircraft_types']:
                top_3 = list(stats['top_aircraft_types'].items())[:3]
                types_str = ', '.join([f"{t}({c})" for t, c in top_3])
                print(f"        主要机型: {types_str}")
        
        print()
        print("验证结果:")
        checks = validation['validation_checks']
        print(f"重型机比例合理: {'✓' if checks['heavy_ratio_reasonable'] else '✗'}")
        print(f"中型机比例合理: {'✓' if checks['medium_ratio_reasonable'] else '✗'}")
        print(f"无未分类飞机: {'✓' if checks['no_unclassified'] else '✗'}")

# 创建默认分类器实例
default_classifier = AircraftClassifier()
