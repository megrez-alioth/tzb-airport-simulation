#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
时间处理工具 - 处理各种时间格式和计算
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Union, Optional

class TimeUtils:
    """时间处理工具类"""
    
    @staticmethod
    def parse_time_string(time_str: Union[str, pd.Timestamp, datetime]) -> Optional[datetime]:
        """
        解析各种格式的时间字符串
        
        Args:
            time_str: 时间字符串或时间对象
            
        Returns:
            datetime: 解析后的datetime对象，失败返回None
        """
        if pd.isna(time_str) or time_str is None:
            return None
            
        # 如果已经是datetime对象
        if isinstance(time_str, (datetime, pd.Timestamp)):
            return pd.to_datetime(time_str).to_pydatetime()
        
        try:
            # 尝试pandas的通用解析
            return pd.to_datetime(time_str).to_pydatetime()
        except:
            try:
                # 尝试常见格式
                common_formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d %H:%M',
                    '%m/%d/%Y %H:%M:%S',
                    '%m/%d/%Y %H:%M',
                    '%H:%M:%S',
                    '%H:%M'
                ]
                
                for fmt in common_formats:
                    try:
                        return datetime.strptime(str(time_str), fmt)
                    except:
                        continue
                
                return None
            except:
                return None
    
    @staticmethod
    def time_to_minutes(time_obj: Union[datetime, timedelta]) -> float:
        """
        将时间对象转换为分钟数
        
        Args:
            time_obj: datetime或timedelta对象
            
        Returns:
            float: 分钟数
        """
        if isinstance(time_obj, timedelta):
            return time_obj.total_seconds() / 60
        elif isinstance(time_obj, datetime):
            # 转换为从午夜开始的分钟数
            return time_obj.hour * 60 + time_obj.minute + time_obj.second / 60
        else:
            return 0
    
    @staticmethod
    def minutes_to_timedelta(minutes: float) -> timedelta:
        """
        将分钟数转换为timedelta对象
        
        Args:
            minutes: 分钟数
            
        Returns:
            timedelta: 时间差对象
        """
        return timedelta(minutes=minutes)
    
    @staticmethod
    def calculate_time_difference(time1: datetime, time2: datetime) -> timedelta:
        """
        计算两个时间的差值
        
        Args:
            time1: 第一个时间
            time2: 第二个时间
            
        Returns:
            timedelta: 时间差 (time1 - time2)
        """
        if time1 is None or time2 is None:
            return timedelta(0)
        return time1 - time2
    
    @staticmethod
    def add_random_variation(base_time: datetime, mean_minutes: float, 
                           std_minutes: float = 0) -> datetime:
        """
        为基础时间添加随机变化
        
        Args:
            base_time: 基础时间
            mean_minutes: 平均增加的分钟数
            std_minutes: 标准差分钟数
            
        Returns:
            datetime: 调整后的时间
        """
        if std_minutes > 0:
            random_minutes = np.random.normal(mean_minutes, std_minutes)
        else:
            random_minutes = mean_minutes
            
        return base_time + timedelta(minutes=random_minutes)
    
    @staticmethod
    def round_to_hour(time_obj: datetime) -> datetime:
        """
        将时间四舍五入到最近的小时
        
        Args:
            time_obj: 输入时间
            
        Returns:
            datetime: 四舍五入后的时间
        """
        if time_obj.minute >= 30:
            return time_obj.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else:
            return time_obj.replace(minute=0, second=0, microsecond=0)
    
    @staticmethod
    def get_hour_of_day(time_obj: datetime) -> int:
        """
        获取一天中的小时数 (0-23)
        
        Args:
            time_obj: 输入时间
            
        Returns:
            int: 小时数
        """
        return time_obj.hour
    
    @staticmethod
    def is_same_day(time1: datetime, time2: datetime) -> bool:
        """
        检查两个时间是否在同一天
        
        Args:
            time1: 第一个时间
            time2: 第二个时间
            
        Returns:
            bool: 是否同一天
        """
        return time1.date() == time2.date()
    
    @staticmethod
    def format_duration(duration: timedelta) -> str:
        """
        格式化时间差显示
        
        Args:
            duration: 时间差
            
        Returns:
            str: 格式化的字符串
        """
        total_seconds = int(duration.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
    
    @staticmethod
    def create_time_series(start_time: datetime, end_time: datetime, 
                          interval_minutes: int = 60) -> list:
        """
        创建时间序列
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            interval_minutes: 间隔分钟数
            
        Returns:
            list: 时间序列列表
        """
        times = []
        current_time = start_time
        
        while current_time <= end_time:
            times.append(current_time)
            current_time += timedelta(minutes=interval_minutes)
            
        return times

# 创建工具实例
time_utils = TimeUtils()
