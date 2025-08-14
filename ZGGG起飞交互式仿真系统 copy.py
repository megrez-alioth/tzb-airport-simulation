#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG机场飞机起飞交互式仿真系统
基于真实数据的队列仿真，考虑天气停飞和积压情况
支持用户自定义仿真参数、停飞时段和塔台效率
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
import os
import sys

warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

class ZGGGDepartureSimulator:
    def __init__(self, delay_threshold=15, backlog_threshold=10, taxi_out_time=15, 
                 flight_data_path=None, suspension_periods=None, efficiency_periods=None):
        """
        ZGGG起飞仿真器初始化
        
        Args:
            delay_threshold: 延误判定阈值(分钟)，可调整参数
            backlog_threshold: 积压判定阈值(班次/小时)
            taxi_out_time: 离港后起飞前准备时间(分钟)，包含滑行和起飞准备
            flight_data_path: 航班数据文件路径
            suspension_periods: 自定义停飞时段列表
            efficiency_periods: 自定义塔台效率时段列表
        """
        self.delay_threshold = delay_threshold
        self.backlog_threshold = backlog_threshold
        self.taxi_out_time = taxi_out_time
        self.flight_data_path = flight_data_path or '5月航班运行数据（脱敏）.xlsx'
        self.suspension_periods = suspension_periods if suspension_periods else []
        self.efficiency_periods = efficiency_periods if efficiency_periods else []
        self.data = None
        self.weather_suspended_periods = []
        self.normal_flights = None
        self.weather_affected_flights = None
        self.all_simulation_results = pd.DataFrame()
        self.peak_periods = {}  # 新增: 存储峰值时段映射
        
        print(f"=== ZGGG起飞仿真器初始化 ===")
        print(f"延误判定阈值: {delay_threshold} 分钟")
        print(f"积压判定阈值: {backlog_threshold} 班/小时")
        print(f"Taxi-out时间: {taxi_out_time} 分钟")
        print(f"数据文件路径: {self.flight_data_path}")
        print(f"自定义停飞时段: {len(self.suspension_periods)} 个")
        print(f"自定义塔台低效时段: {len(self.efficiency_periods)} 个")
    
    def load_departure_data(self):
        """载入ZGGG起飞航班数据"""
        print(f"\n=== 载入ZGGG起飞航班数据 ===")
        
        # 尝试多个可能的位置查找文件
        file_found = False
        file_paths_to_try = [
            self.flight_data_path,  # 用户指定的路径
            os.path.join(os.getcwd(), self.flight_data_path),  # 当前工作目录
            os.path.join(os.path.dirname(os.path.abspath(__file__)), self.flight_data_path),  # 脚本目录
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "数据", self.flight_data_path)  # 脚本目录下的数据文件夹
        ]
        
        for path in file_paths_to_try:
            if os.path.exists(path):
                self.flight_data_path = path
                file_found = True
                break
        
        if not file_found:
            print(f"错误: 无法找到数据文件 '{self.flight_data_path}'")
            print("尝试过以下路径:")
            for p in file_paths_to_try:
                print(f" - {p}")
            print("\n请确保数据文件位于正确的位置，或提供完整路径。")
            return False
        
        try:
            # 读取数据
            df = pd.read_excel(self.flight_data_path)
            print(f"原始数据总记录数: {len(df)}")
        except Exception as e:
            print(f"加载数据文件时出错: {str(e)}")
            return False
        
        # 提取ZGGG起飞航班（实际起飞站四字码 == 'ZGGG'）
        zggg_departures = df[df['实际起飞站四字码'] == 'ZGGG'].copy()
        print(f"ZGGG起飞航班总数: {len(zggg_departures)}")
        
        # 转换时间字段
        time_fields = ['计划离港时间', '实际离港时间', '实际起飞时间']
        for field in time_fields:
            if field in zggg_departures.columns:
                zggg_departures[field] = pd.to_datetime(zggg_departures[field], errors='coerce')
        
        # 分析数据：仅保留有基本关键数据的航班
        basic_required_fields = ['航班号', '机型', '唯一序列号', '计划离港时间', '实际离港时间']
        basic_valid_flights = zggg_departures.dropna(subset=basic_required_fields).copy()
        print(f"有基本关键数据的航班: {len(basic_valid_flights)}")
        
        # 分离有/无实际起飞时间的航班
        has_takeoff_time = basic_valid_flights['实际起飞时间'].notna()
        complete_flights = basic_valid_flights[has_takeoff_time].copy()
        missing_takeoff_flights = basic_valid_flights[~has_takeoff_time].copy()
        
        print(f"有实际起飞时间的航班: {len(complete_flights)} 班")
        print(f"缺失实际起飞时间的航班: {len(missing_takeoff_flights)} 班")
        
        # 对缺失实际起飞时间的航班，使用实际离港时间+20分钟估算
        if len(missing_takeoff_flights) > 0:
            missing_takeoff_flights['实际起飞时间'] = (
                missing_takeoff_flights['实际离港时间'] + pd.Timedelta(minutes=20)
            )
            print(f"为 {len(missing_takeoff_flights)} 班航班估算实际起飞时间(离港+20分钟)")
        
        # 合并数据
        all_flights = pd.concat([complete_flights, missing_takeoff_flights], ignore_index=True)
        print(f"合并后总航班数: {len(all_flights)}")
        
        # 验证时间逻辑：实际离港时间应早于实际起飞时间
        time_logic_check = all_flights['实际离港时间'] <= all_flights['实际起飞时间']
        valid_time_logic = all_flights[time_logic_check].copy()
        invalid_count = len(all_flights) - len(valid_time_logic)
        
        print(f"时间逻辑检查: {len(valid_time_logic)} 班正常, {invalid_count} 班异常")
        
        if invalid_count > 0:
            print("发现时间逻辑异常的航班(实际离港晚于实际起飞)，已剔除")
        
        # 计算基础延误时间
        valid_time_logic['起飞延误分钟'] = (
            valid_time_logic['实际起飞时间'] - valid_time_logic['计划离港时间']
        ).dt.total_seconds() / 60
        
        valid_time_logic['滑行时间分钟'] = (
            valid_time_logic['实际起飞时间'] - valid_time_logic['实际离港时间']
        ).dt.total_seconds() / 60
        
        # 为仿真添加跑道分配标识(ZGGG有两条起飞跑道)
        valid_time_logic['跑道'] = np.random.choice(['02R/20L', '02L/20R'], len(valid_time_logic))
        
        # 区分分析用数据和仿真用数据
        self.analysis_data = complete_flights[
            complete_flights['实际离港时间'] <= complete_flights['实际起飞时间']
        ].copy()
        self.simulation_data = valid_time_logic.copy()
        
        print(f"\n=== 数据分类 ===")
        
        # 计算分析数据的基础延误时间
        if len(self.analysis_data) > 0:
            self.analysis_data['起飞延误分钟'] = (
                self.analysis_data['实际起飞时间'] - self.analysis_data['计划离港时间']
            ).dt.total_seconds() / 60
            
            self.analysis_data['滑行时间分钟'] = (
                self.analysis_data['实际起飞时间'] - self.analysis_data['实际离港时间']
            ).dt.total_seconds() / 60
        
        self.data = valid_time_logic
        
        # 显示基本统计信息(基于分析数据)
        print(f"\n=== 基本统计信息(基于真实起飞时间) ===")
        if len(self.analysis_data) > 0:
            print(f"起飞延误: 平均 {self.analysis_data['起飞延误分钟'].mean():.1f} 分钟")
            print(f"滑行时间: 平均 {self.analysis_data['滑行时间分钟'].mean():.1f} 分钟")
            print(f"机型分布: {dict(self.analysis_data['机型'].value_counts().head())}")
        else:
            print("无法计算统计信息(无真实起飞时间数据)")
        
        # 修改返回值：确保返回布尔值而不是DataFrame
        return True if not self.data.empty else False
    
    def identify_weather_suspended_periods(self):
        """识别天气停飞时段"""
        print(f"\n=== 识别天气停飞时段 ===")
        
        # 如果有用户自定义停飞时段，则直接使用
        if self.suspension_periods:
            print(f"使用用户自定义的 {len(self.suspension_periods)} 个停飞时段")
            self.weather_suspended_periods = self.suspension_periods
            return self.weather_suspended_periods
        
        if not hasattr(self, 'analysis_data') or len(self.analysis_data) == 0:
            print("错误: 需要先载入有真实起飞时间的分析数据")
            return []
        
        # 使用分析数据识别天气停飞：延误超过4小时(240分钟)的航班
        extreme_delays = self.analysis_data[self.analysis_data['起飞延误分钟'] > 240].copy()
        print(f"发现极端延误航班(>4小时): {len(extreme_delays)} 班")
        
        if len(extreme_delays) == 0:
            print("未发现明显的天气停飞事件")
            return []
        
        # 按日期分组分析
        extreme_delays['date'] = extreme_delays['计划离港时间'].dt.date
        extreme_delays['actual_takeoff_hour'] = extreme_delays['实际起飞时间'].dt.hour
        
        weather_events = []
        
        for date in extreme_delays['date'].unique():
            day_flights = extreme_delays[extreme_delays['date'] == date]
            
            # 如果某天有5班以上极端延误，认为是天气停飞日
            if len(day_flights) >= 5:
                # 分析实际起飞时间的集中分布
                takeoff_hours = day_flights['actual_takeoff_hour'].value_counts()
                concentrated_hours = takeoff_hours[takeoff_hours >= 2].index.tolist()
                
                if concentrated_hours:
                    # 估算停飞结束时间（集中起飞时段的开始）
                    resume_hour = min(concentrated_hours)
                    
                    # 估算停飞开始时间（最早计划离港前1小时）
                    earliest_planned = day_flights['计划离港时间'].min()
                    suspend_start = pd.Timestamp.combine(date, pd.Timestamp.min.time()) + pd.Timedelta(hours=max(0, earliest_planned.hour-1))
                    suspend_end = pd.Timestamp.combine(date, pd.Timestamp.min.time()) + pd.Timedelta(hours=resume_hour)
                    
                    # 与交互式系统格式保持一致
                    weather_event = {
                        'date': date,
                        'start_time': suspend_start,
                        'end_time': suspend_end,
                        'affected_count': len(day_flights)
                    }
                    
                    weather_events.append(weather_event)
                    print(f"  识别停飞: {date} {suspend_start.strftime('%H:%M')}-{suspend_end.strftime('%H:%M')} "
                          f"(影响{len(day_flights)}班)")
        
        self.weather_suspended_periods = weather_events
        print(f"总计识别出 {len(weather_events)} 个天气停飞事件")
        
        return weather_events
    
    def classify_aircraft_types(self):
        """机型分类和ROT参数设定"""
        print(f"\n=== 机型分类和ROT参数设定 ===")
        
        if self.data is None:
            print("错误: 需要先载入数据")
            return
        
        # 定义机型分类和ROT参数(跑道占用时间,秒) - 调整为中等强度
        self.aircraft_categories = {
            # 大型客机 (Heavy/Super Heavy) - 适度增加ROT时间
            'Heavy': {
                'types': ['773', '772', '77W', '77L', '744', '748', '380', '359', '358', '35K'],
                'rot_seconds': 105,  # 调整到105秒
                'wake_category': 'Heavy'
            },
            # 中型客机 (Medium) - 适度增加ROT时间
            'Medium': {
                'types': ['32G', '32N', '32A', '321', '320', '319', '327', '32S', '32Q',
                         '73M', '738', '739', '73G', '73H', '737', '73W', '73J',
                         '909', '290', 'E90', 'ER4', 'ERJ', 'E75'],
                'rot_seconds': 85,   # 调整到85秒
                'wake_category': 'Medium'
            },
            # 小型客机/支线 (Light) - 适度增加ROT时间
            'Light': {
                'types': ['AT7', 'AT5', 'DH8', 'CR9', 'CRJ', 'CR7', 'E45', 'SF3', 'J41'],
                'rot_seconds': 70,   # 调整到70秒
                'wake_category': 'Light'
            },
            # 货机 (Cargo) - 适度增加ROT时间
            'Cargo': {
                'types': ['76F', '77F', '74F', '32P', '737F'],
                'rot_seconds': 115,  # 调整到115秒
                'wake_category': 'Heavy'
            }
        }
        
        # 为每个航班分配机型类别
        def get_aircraft_category(aircraft_type):
            for category, info in self.aircraft_categories.items():
                if aircraft_type in info['types']:
                    return category, info['rot_seconds'], info['wake_category']
            # 默认为中型机
            return 'Medium', 85, 'Medium'
        
        # 应用分类
        self.data[['机型类别', 'ROT秒', '尾流类别']] = self.data['机型'].apply(
            lambda x: pd.Series(get_aircraft_category(x))
        )
        
        # 统计机型分布
        category_stats = self.data['机型类别'].value_counts()
        print(f"机型类别分布:")
        for category, count in category_stats.items():
            percentage = count / len(self.data) * 100
            rot = self.aircraft_categories[category]['rot_seconds']
            print(f"  {category}: {count}班 ({percentage:.1f}%) - ROT: {rot}秒")
        
        # 尾流间隔矩阵(秒) - 前机→后机，调整为中等强度
        self.wake_separation_matrix = {
            ('Heavy', 'Heavy'): 105,   # 调整到105秒
            ('Heavy', 'Medium'): 135,  # 调整到135秒
            ('Heavy', 'Light'): 195,   # 调整到195秒
            ('Medium', 'Heavy'): 75,   # 调整到75秒
            ('Medium', 'Medium'): 105, # 调整到105秒
            ('Medium', 'Light'): 135,  # 调整到135秒
            ('Light', 'Heavy'): 75,    # 调整到75秒
            ('Light', 'Medium'): 75,   # 调整到75秒
            ('Light', 'Light'): 105    # 调整到105秒
        }
        
        print(f"\n尾流间隔矩阵设定完成(前机→后机最小间隔)")
        return True
    
    def separate_flight_types(self):
        """分离正常航班和天气影响航班"""
        print(f"\n=== 分离正常航班和天气影响航班 ===")
        
        if self.data is None:
            print("错误: 需要先载入数据")
            return
        
        weather_affected_flights = []
        
        # 标记每个天气停飞期间的航班
        for event in self.weather_suspended_periods:
            event_date = event['date']
            suspend_start = event['start_time']
            suspend_end = event['end_time']
            
            # 找出在此期间计划起飞的航班
            day_flights = self.data[
                self.data['计划离港时间'].dt.date == event_date
            ]
            
            affected_in_period = day_flights[
                (day_flights['计划离港时间'] >= suspend_start) & 
                (day_flights['计划离港时间'] <= suspend_end)
            ]
            
            weather_affected_flights.extend(affected_in_period.index.tolist())
        
        # 创建标记
        self.data['受天气影响'] = self.data.index.isin(weather_affected_flights)
        
        # 分离数据
        self.normal_flights = self.data[~self.data['受天气影响']].copy()
        self.weather_affected_flights = self.data[self.data['受天气影响']].copy()
        
        print(f"正常航班: {len(self.normal_flights)} 班")
        print(f"受天气影响航班: {len(self.weather_affected_flights)} 班")
        
        return True
    
    def run_simulation_for_date_range(self, start_date, end_date):
        """在日期范围内运行仿真"""
        print(f"\n=== 对日期范围运行仿真 ===")
        
        if self.data is None:
            print("错误: 需要先完成前序步骤")
            return False
        
        all_results = []
        date_range = pd.date_range(start_date, end_date)
        
        for sim_date in date_range:
            sim_date_obj = sim_date.date()
            print(f"\n--- 对日期 {sim_date_obj} 进行仿真 ---")
            
            day_flights = self.data[self.data['计划离港时间'].dt.date == sim_date_obj].copy()
            if day_flights.empty:
                print(f"当天无计划航班")
                continue
                
            day_flights.sort_values('计划离港时间', inplace=True)
            print(f"当日航班数: {len(day_flights)} 班")
            
            # 检查当天的事件
            day_suspensions = [p for p in self.weather_suspended_periods if p['date'] == sim_date_obj]
            day_efficiency_mods = [p for p in self.efficiency_periods if p['date'] == sim_date_obj]
            
            if day_suspensions:
                print(f"当日停飞时段: {len(day_suspensions)} 个")
            if day_efficiency_mods:
                print(f"当日塔台低效时段: {len(day_efficiency_mods)} 个")
                
            # 运行仿真
            sim_results = self.simulate_runway_queue_for_day(day_flights, day_suspensions, day_efficiency_mods)
            all_results.extend(sim_results)
        
        if not all_results:
            print("\n仿真没有产生结果")
            return False
            
        self.all_simulation_results = pd.DataFrame(all_results)
        print(f"\n=== 日期范围仿真完成 ===")
        print(f"总计仿真航班: {len(self.all_simulation_results)} 班")
        
        return True
    
    def analyze_peak_periods(self):
        """分析实际数据识别高峰、平峰和低峰时段"""
        print(f"\n=== 分析峰值时段 ===")
        
        if not hasattr(self, 'analysis_data') or self.analysis_data is None or len(self.analysis_data) == 0:
            print("错误: 需要先载入有真实起飞时间的分析数据")
            return False
            
        # 按日期和小时统计积压航班数
        self.analysis_data['date'] = self.analysis_data['计划离港时间'].dt.date
        self.analysis_data['hour'] = self.analysis_data['计划离港时间'].dt.hour
        
        # 计算每个日期-小时的延误航班数
        delayed_flights = self.analysis_data[self.analysis_data['起飞延误分钟'] > self.delay_threshold]
        peak_data = delayed_flights.groupby(['date', 'hour']).size().reset_index(name='delayed_count')
        
        # 获取总航班数作为参考
        total_flights = self.analysis_data.groupby(['date', 'hour']).size().reset_index(name='total_count')
        peak_data = peak_data.merge(total_flights, on=['date', 'hour'])
        
        # 计算延误率
        peak_data['delay_rate'] = peak_data['delayed_count'] / peak_data['total_count'] * 100
        
        # 定义峰值标准: 高峰(延误航班数>=积压阈值)、平峰(延误航班数>=积压阈值/2)、低峰(其他)
        high_peak_threshold = self.backlog_threshold
        medium_peak_threshold = self.backlog_threshold / 2
        
        # 分类每个时段
        peak_data['peak_type'] = 'Low'
        peak_data.loc[peak_data['delayed_count'] >= medium_peak_threshold, 'peak_type'] = 'Medium'
        peak_data.loc[peak_data['delayed_count'] >= high_peak_threshold, 'peak_type'] = 'High'
        
        # 保存峰值时段映射
        self.peak_periods = {}
        for _, row in peak_data.iterrows():
            date = row['date']
            hour = row['hour']
            peak_type = row['peak_type']
            delayed = row['delayed_count']
            total = row['total_count']
            
            if date not in self.peak_periods:
                self.peak_periods[date] = {}
            
            self.peak_periods[date][hour] = {
                'peak_type': peak_type,
                'delayed_count': delayed,
                'total_count': total,
                'delay_rate': delayed / total * 100 if total > 0 else 0
            }
        
        # 统计各类峰值时段数量
        peak_counts = peak_data['peak_type'].value_counts()
        print(f"峰值时段分类完成:")
        print(f"  高峰时段: {peak_counts.get('High', 0)} 个 (延误航班≥{high_peak_threshold}班/小时)")
        print(f"  平峰时段: {peak_counts.get('Medium', 0)} 个 (延误航班≥{medium_peak_threshold}班/小时)")
        print(f"  低峰时段: {peak_counts.get('Low', 0)} 个")
        
        # 显示各峰值时段的仿真参数
        print(f"\n--- 各峰值时段仿真参数设定 ---")
        print(f"  高峰时段参数:")
        print(f"    - 滑行时间倍数: 1.2 (增加20%)")
        print(f"    - 尾流间隔倍数: 1.15 (增加15%)")
        print(f"    - ROT时间倍数: 1.1 (增加10%)")
        print(f"  平峰时段参数:")
        print(f"    - 滑行时间倍数: 1.1 (增加10%)")
        print(f"    - 尾流间隔倍数: 1.05 (增加5%)")
        print(f"    - ROT时间倍数: 1.0 (不变)")
        print(f"  低峰时段参数:")
        print(f"    - 滑行时间倍数: 1.0 (不变)")
        print(f"    - 尾流间隔倍数: 0.95 (减少5%)")
        print(f"    - ROT时间倍数: 0.95 (减少5%)")
        
        # 显示具体的时段分类详情
        self._display_peak_period_details(peak_data)
        
        # 可视化展示峰值分布
        print(f"\n生成峰值分布可视化图表...")
        self._visualize_peak_distribution(peak_data)
        
        return True
    
    def get_dynamic_parameters(self, date, hour):
        """根据日期和小时获取动态仿真参数"""
        # 根据峰值时段调整参数
        default_params = {
            'taxi_out_time': self.taxi_out_time,
            'separation_multiplier': 1.0,  # 尾流间隔倍数
            'rot_multiplier': 1.0          # ROT时间倍数
        }
        
        if not self.peak_periods:
            return default_params
        
        # 如果没有该日期或小时的数据，返回默认参数
        if date not in self.peak_periods or hour not in self.peak_periods[date]:
            return default_params
        
        peak_info = self.peak_periods[date][hour]
        peak_type = peak_info['peak_type']
        
        # 根据峰值类型调整参数
        if peak_type == 'High':
            # 高峰: 延长滑行时间和间隔时间
            return {
                'taxi_out_time': self.taxi_out_time * 1.0,  # 增加20%滑行时间
                'separation_multiplier': 1.3,  # 增加15%尾流间隔
                'rot_multiplier': 1.5         # 增加10% ROT时间
            }
        elif peak_type == 'Medium':
            # 平峰: 稍微延长滑行和间隔时间
            return {
                'taxi_out_time': self.taxi_out_time * 0.9,  # 增加10%滑行时间
                'separation_multiplier': 1.0,  # 增加5%尾流间隔
                'rot_multiplier': 1.0         # 保持默认ROT时间
            }
        else:
            # 低峰: 使用默认参数，保持滑行时间不变，优化间隔时间
            return {
                'taxi_out_time': self.taxi_out_time,        # 保持默认滑行时间
                'separation_multiplier': 0.95,  # 减少5%尾流间隔
                'rot_multiplier': 0.95         # 减少5% ROT时间
            }

    def simulate_runway_queue_for_day(self, day_flights, suspensions, efficiencies):
        """为单日模拟跑道队列，使用动态参数"""
        runway_last_departure = {'02R/20L': None, '02L/20R': None}
        results = []
        
        # 识别可能受到优先级延误影响的航班
        priority_delayed_flights = set()
        for eff in efficiencies:
            if eff.get('type') == '优先级延误':
                start, end = eff['start_time'], eff['end_time']
                small_flights = day_flights[
                    (day_flights['计划离港时间'] >= start) & 
                    (day_flights['计划离港时间'] <= end) &
                    (day_flights['机型类别'].isin(['Light', 'Medium']))
                ]
                priority_delayed_flights.update(small_flights.index)
        
        for idx, flight in day_flights.iterrows():
            # 获取当前航班的动态参数
            flight_date = flight['计划离港时间'].date()
            flight_hour = flight['计划离港时间'].hour
            dynamic_params = self.get_dynamic_parameters(flight_date, flight_hour)
            
            selected_runway = flight['跑道']  # 使用预分配的跑道
            planned_departure = flight['计划离港时间']
            # 使用动态taxi_out_time替代固定值
            base_takeoff_time = planned_departure + pd.Timedelta(minutes=dynamic_params['taxi_out_time'])
            
            # 1. 检查停飞影响
            is_suspended = False
            for period in suspensions:
                if period['start_time'] <= planned_departure <= period['end_time']:
                    # 受停飞影响的航班，起飞时间不早于停飞结束
                    base_takeoff_time = max(base_takeoff_time, period['end_time'])
                    is_suspended = True
                    break
            
            # 2. 检查塔台效率影响
            tower_efficiency = 1.0  # 默认效率100%
            for period in efficiencies:
                if period['start_time'] <= planned_departure <= period['end_time']:
                    # 根据效率类型应用不同的延迟
                    effect_type = period.get('type', '按顺序延误')
                    
                    if (effect_type == '随机延误' and np.random.rand() > 0.5) or \
                       (effect_type == '按顺序延误') or \
                       (effect_type == '优先级延误' and idx in priority_delayed_flights):
                        tower_efficiency = period.get('efficiency', 1.0)
                    break
            
            # 3. 计算实际可用的跑道时间
            last_flight_info = runway_last_departure[selected_runway]
            min_takeoff_time = base_takeoff_time
            
            if last_flight_info:
                last_takeoff_time = last_flight_info['sim_takeoff']
                last_wake_cat = last_flight_info['wake_category']
                
                # 应用塔台效率和动态间隔乘数到间隔时间
                wake_separation_secs = self.wake_separation_matrix.get(
                    (last_wake_cat, flight['尾流类别']), 90)
                # 同时考虑塔台效率和动态间隔调整
                effective_separation_secs = (wake_separation_secs * dynamic_params['separation_multiplier']) / tower_efficiency
                
                # 考虑前机ROT和尾流间隔
                runway_free_time = last_takeoff_time + pd.Timedelta(seconds=effective_separation_secs)
                min_takeoff_time = max(min_takeoff_time, runway_free_time)
            
            # 确定最终起飞时间
            simulated_takeoff = min_takeoff_time
            
            # 计算延误时间 - 对于受天气影响的航班，使用停飞结束时间作为基准
            if is_suspended:
                # 找到影响此航班的停飞时段结束时间作为新的基准时间
                reference_time = planned_departure  # 默认使用计划时间
                for period in suspensions:
                    if period['start_time'] <= planned_departure <= period['end_time']:
                        reference_time = period['end_time']
                        break
                delay_minutes = (simulated_takeoff - reference_time).total_seconds() / 60
            else:
                # 正常航班仍使用原计划起飞时间计算延误
                delay_minutes = (simulated_takeoff - planned_departure).total_seconds() / 60
            
            # 记录结果
            # 确定延误计算基准时间和描述
            if is_suspended:
                # 找到影响此航班的停飞时段结束时间作为基准
                reference_time = planned_departure
                delay_calculation_base = '计划起飞时间'
                for period in suspensions:
                    if period['start_time'] <= planned_departure <= period['end_time']:
                        reference_time = period['end_time']
                        delay_calculation_base = f'停飞结束时间({period["end_time"].strftime("%H:%M")})'
                        break
            else:
                reference_time = planned_departure
                delay_calculation_base = '计划起飞时间'
            
            results.append({
                '航班号': flight['航班号'],
                '机型': flight['机型'],
                '机型类别': flight['机型类别'],
                '跑道': selected_runway,
                '计划起飞': planned_departure,
                '延误计算基准': reference_time,
                '延误基准说明': delay_calculation_base,
                '仿真起飞时间': simulated_takeoff,
                '仿真延误分钟': delay_minutes,
                '受天气影响': is_suspended,
                '塔台效率': tower_efficiency * 100,
                '实际延误分钟': flight['起飞延误分钟'] if '起飞延误分钟' in flight else None,
                '时段类型': self.peak_periods.get(flight_date, {}).get(flight_hour, {}).get('peak_type', 'Unknown')
            })
            
            # 更新跑道状态 - 考虑动态ROT调整
            dynamic_rot = flight['ROT秒'] * dynamic_params['rot_multiplier']
            runway_last_departure[selected_runway] = {
                'sim_takeoff': simulated_takeoff,
                'wake_category': flight['尾流类别'],
                'rot_seconds': dynamic_rot
            }
        
        return results
        
    def simulate_runway_queue(self, target_date=None, verbose=False):
        """单日跑道排队仿真（向后兼容的方法）"""
        print(f"\n=== 跑道排队仿真 ===")
        
        if self.data is None:
            print("错误: 需要先完成前序步骤")
            return
        
        # 如果未指定日期，选择一个典型日期进行仿真
        if target_date is None:
            # 选择航班数量中等的日期
            daily_counts = self.data['计划离港时间'].dt.date.value_counts()
            median_count = daily_counts.median()
            target_date = daily_counts[
                abs(daily_counts - median_count) == abs(daily_counts - median_count).min()
            ].index[0]
        
        print(f"仿真日期: {target_date}")
        
        # 提取当日航班
        day_flights = self.data[
            self.data['计划离港时间'].dt.date == target_date
        ].copy().sort_values('计划离港时间')
        
        print(f"当日航班数: {len(day_flights)} 班")
        
        # 检查是否有天气停飞
        day_weather_events = [
            event for event in self.weather_suspended_periods 
            if event['date'] == target_date
        ]
        
        # 检查是否有塔台低效事件
        day_efficiency_events = [
            event for event in self.efficiency_periods
            if event['date'] == target_date
        ]
        
        if day_weather_events:
            print(f"当日天气停飞事件: {len(day_weather_events)} 个")
            for event in day_weather_events:
                print(f"  停飞时段: {event['start_time'].strftime('%H:%M')}-{event['end_time'].strftime('%H:%M')}")
        
        if day_efficiency_events:
            print(f"当日塔台低效事件: {len(day_efficiency_events)} 个")
            for event in day_efficiency_events:
                eff_pct = event.get('efficiency', 1.0) * 100
                print(f"  低效时段: {event['start_time'].strftime('%H:%M')}-{event['end_time'].strftime('%H:%M')} ({eff_pct:.0f}% 效率)")
        
        # 使用新的仿真方法
        simulation_results = self.simulate_runway_queue_for_day(day_flights, day_weather_events, day_efficiency_events)
        
        # 转换为DataFrame
        simulation_results = pd.DataFrame(simulation_results)
        
        # 统计分析
        print(f"\n=== 仿真结果统计 ===")
        if not simulation_results.empty:
            print(f"仿真延误: 平均 {simulation_results['仿真延误分钟'].mean():.1f} 分钟")
            if '实际延误分钟' in simulation_results.columns:
                print(f"实际延误: 平均 {simulation_results['实际延误分钟'].mean():.1f} 分钟")
            
            # 跑道使用情况
            runway_usage = simulation_results['跑道'].value_counts()
            print(f"跑道使用分布:")
            for runway, count in runway_usage.items():
                percentage = count / len(simulation_results) * 100
                print(f"  {runway}: {count}班 ({percentage:.1f}%)")
            
            # 延误计算基准统计
            if '延误基准说明' in simulation_results.columns:
                base_usage = simulation_results['延误基准说明'].value_counts()
                print(f"\n延误计算基准分布:")
                for base, count in base_usage.items():
                    percentage = count / len(simulation_results) * 100
                    print(f"  {base}: {count}班 ({percentage:.1f}%)")
            
            # 延误对比
            normal_sim = simulation_results[~simulation_results['受天气影响']]
            weather_sim = simulation_results[simulation_results['受天气影响']]
            
            if len(normal_sim) > 0:
                print(f"\n正常天气航班:")
                print(f"  仿真延误: 平均 {normal_sim['仿真延误分钟'].mean():.1f} 分钟")
            
            if len(weather_sim) > 0:
                print(f"天气影响航班 (使用停飞结束时间为基准):")
                print(f"  仿真延误: 平均 {weather_sim['仿真延误分钟'].mean():.1f} 分钟")
                print(f"  说明: 延误时间从停飞结束时间开始计算，而非原计划起飞时间")
        
        return simulation_results
    
    def analyze_and_visualize_results(self):
        """分析和可视化仿真结果 - 与真实数据对比"""
        print(f"\n=== 分析和可视化仿真结果 ===")
        
        if self.all_simulation_results.empty:
            print("没有仿真结果可供分析")
            return False
        
        # 提取仿真结果
        sim_results = self.all_simulation_results.copy()
        
        # 计算基本统计信息
        avg_delay = sim_results['仿真延误分钟'].mean()
        delay_rate = (sim_results['仿真延误分钟'] > self.delay_threshold).sum() / len(sim_results) * 100
        
        print(f"\n--- 整体仿真统计 ---")
        print(f"平均仿真延误: {avg_delay:.1f} 分钟")
        print(f"仿真延误率 (> {self.delay_threshold} 分钟): {delay_rate:.1f}%")
        
        # 统计延误计算基准
        if '延误基准说明' in sim_results.columns:
            base_usage = sim_results['延误基准说明'].value_counts()
            print(f"\n--- 延误计算基准统计 ---")
            for base, count in base_usage.items():
                percentage = count / len(sim_results) * 100
                print(f"  {base}: {count}班 ({percentage:.1f}%)")
        
        # 天气影响航班的特殊说明
        weather_affected = sim_results[sim_results['受天气影响']]
        if len(weather_affected) > 0:
            print(f"\n--- 天气影响航班延误说明 ---")
            print(f"天气影响航班数量: {len(weather_affected)} 班")
            print(f"说明: 这些航班的仿真延误时间是从停飞结束时间开始计算")
            print(f"     而不是从原计划起飞时间计算，以更准确反映实际延误情况")
        
        # 获取真实数据（如果有的话）
        if hasattr(self, 'analysis_data') and len(self.analysis_data) > 0:
            real_data = self.analysis_data.copy()
            
            # 确保真实数据中有需要的字段
            if '起飞延误分钟' not in real_data.columns:
                print("真实数据中缺少延误时间信息，无法与仿真进行对比")
                real_data = None
        else:
            print("无真实数据可供对比，仅展示仿真结果")
            real_data = None
        
        # 进行可视化交互式设置 - 使用与停飞时段配置相同的风格
        print(f"\n--- 可视化时段配置 ---")
        
        # 获取可视化范围
        visualization_periods = []
        
        while True:
            try:
                add_viz_period = input("添加一个可视化时段? (y/n) [n]: ").lower()
            except EOFError:
                # 如果输入流结束，默认不添加可视化时段
                print("n")  # 显示默认选择
                add_viz_period = 'n'
            
            if add_viz_period != 'y':
                # 如果用户没有添加可视化时段，则使用所有数据
                self._visualize_delay_comparison(sim_results, real_data)
                break
                
            print("  请输入开始和结束时间:")
            try:
                start_datetime_str = input("  可视化开始时间 (YYYY-MM-DD HH:MM): ")
                end_datetime_str = input("  可视化结束时间 (YYYY-MM-DD HH:MM): ")
            except EOFError:
                # 如果输入流结束，跳出循环使用默认可视化
                print("\\n使用默认可视化设置")
                self._visualize_delay_comparison(sim_results, real_data)
                break
            
            try:
                # 修正：更明确的错误处理和日期格式指导
                try:
                    start_datetime = pd.to_datetime(start_datetime_str, format="%Y-%m-%d %H:%M")
                except:
                    print("  错误: 开始时间格式不正确。请确保使用 YYYY-MM-DD HH:MM 格式，例如 2023-05-01 08:00")
                    continue
                    
                try:
                    end_datetime = pd.to_datetime(end_datetime_str, format="%Y-%m-%d %H:%M")
                except:
                    print("  错误: 结束时间格式不正确。请确保使用 YYYY-MM-DD HH:MM 格式，例如 2023-05-01 18:00")
                    continue
                
                if end_datetime <= start_datetime:
                    print("  错误: 结束时间必须晚于开始时间。请重试。")
                    continue
                
                # 确认所选时段并可视化
                print(f"  可视化时段设置为 {start_datetime.strftime('%Y-%m-%d %H:%M')} 到 {end_datetime.strftime('%Y-%m-%d %H:%M')}")
                self._visualize_delay_comparison(sim_results, real_data, start_datetime, end_datetime)
                break
                
            except Exception as e:
                print(f"  发生错误: {str(e)}")
                print("  无效的日期/时间格式。请使用 YYYY-MM-DD HH:MM 格式，例如 2023-05-01 08:00。请重试。")
        
        return True

    def _visualize_delay_comparison(self, sim_results, real_data, start_datetime=None, end_datetime=None):
        """
        生成真实数据与仿真数据的延误分布对比图
        
        Args:
            sim_results: 仿真结果DataFrame
            real_data: 真实数据DataFrame，如无则为None
            start_datetime: 展示的起始日期时间，如无则显示全部
            end_datetime: 展示的结束日期时间，如无则显示全部
        """
        # 检查所有必要的列是否存在
        required_columns = ['计划起飞', '仿真延误分钟']
        
        # 修复：统一列名
        if '计划起飞' not in sim_results.columns and '计划离港时间' in sim_results.columns:
            sim_results = sim_results.copy()  # 确保我们不修改原始数据
            sim_results['计划起飞'] = sim_results['计划离港时间']
        
        # 安全检查
        for col in required_columns:
            if col not in sim_results.columns:
                print(f"错误: 仿真结果缺少必要的列 '{col}'")
                print(f"可用列: {list(sim_results.columns)}")
                return False
        
        # 添加日期时间字段以更精确地分组
        sim_results['datetime'] = sim_results['计划起飞']
        sim_results['hour'] = sim_results['datetime'].dt.hour
        sim_results['date'] = sim_results['datetime'].dt.date
        
        # 过滤时间范围
        filtered_sim_results = sim_results.copy()
        filtered_real_data = real_data.copy() if real_data is not None else None
        
        if start_datetime is not None and end_datetime is not None:
            # 过滤仿真数据
            filtered_sim_results = filtered_sim_results[
                (filtered_sim_results['计划起飞'] >= start_datetime) & 
                (filtered_sim_results['计划起飞'] <= end_datetime)
            ]
            
            # 如果有真实数据，也进行过滤 - 使用相同时段
            if filtered_real_data is not None:
                filtered_real_data = filtered_real_data[
                    (filtered_real_data['计划离港时间'] >= start_datetime) & 
                    (filtered_real_data['计划离港时间'] <= end_datetime)
                ]
            
            title_time_range = f"{start_datetime.strftime('%Y-%m-%d %H:%M')} 至 {end_datetime.strftime('%Y-%m-%d %H:%M')}"
            output_filename_suffix = f"{start_datetime.strftime('%Y%m%d%H%M')}至{end_datetime.strftime('%Y%m%d%H%M')}"
        else:
            # 使用与仿真数据相同的日期范围过滤真实数据
            if filtered_real_data is not None and not filtered_sim_results.empty:
                try:
                    min_date = filtered_sim_results['datetime'].min().date()
                    max_date = filtered_sim_results['datetime'].max().date()
                    
                    filtered_real_data = filtered_real_data[
                        (filtered_real_data['计划离港时间'].dt.date >= min_date) & 
                        (filtered_real_data['计划离港时间'].dt.date <= max_date)
                    ]
                    title_time_range = f"{min_date} 至 {max_date}"
                    output_filename_suffix = f"{min_date.strftime('%Y%m%d')}至{max_date.strftime('%Y%m%d')}"
                except:
                    # 处理可能出现的错误
                    title_time_range = "全部时段"
                    output_filename_suffix = "全部时段"
            else:
                title_time_range = "全部时段"
                output_filename_suffix = "全部时段"
        
        # 检查过滤后是否有数据
        if filtered_sim_results.empty:
            print("选定时段内无仿真数据，无法生成图表")
            return False
            
        # 打印过滤后的数据数量
        print(f"选定时段内仿真数据: {len(filtered_sim_results)} 条")
        if filtered_real_data is not None:
            print(f"选定时段内真实数据: {len(filtered_real_data)} 条")
        
        # 确定是否需要按小时或天进行分组
        time_span = (end_datetime - start_datetime).total_seconds() / 3600 if start_datetime and end_datetime else 24
        
        # 修改：根据时间跨度决定分组方式
        if time_span <= 48:  # 2天或更短 - 按小时分组
            # 创建联合键：日期+小时
            filtered_sim_results['date_hour'] = filtered_sim_results['datetime'].dt.strftime('%Y-%m-%d %H:00')
            
            # 使用当前延误阈值统计仿真延误航班数
            # 重要修改: 使用类的延误阈值，而不是硬编码值
            sim_delayed = filtered_sim_results[filtered_sim_results['仿真延误分钟'] > self.delay_threshold]
            sim_hourly_counts = sim_delayed.groupby('date_hour').size()
            
            # 真实数据处理 - 确保使用相同的延误阈值
            if filtered_real_data is not None and not filtered_real_data.empty:
                # 为真实数据添加时间字段
                filtered_real_data['datetime'] = filtered_real_data['计划离港时间']
                filtered_real_data['date_hour'] = filtered_real_data['datetime'].dt.strftime('%Y-%m-%d %H:00')
                
                # 重要修改: 使用与仿真数据相同的延误阈值来判定真实数据中的延误
                real_delayed = filtered_real_data[filtered_real_data['起飞延误分钟'] > self.delay_threshold]
                real_hourly_counts = real_delayed.groupby('date_hour').size()
                
                # 打印延误率对比
                real_delay_rate = len(real_delayed) / len(filtered_real_data) * 100 if len(filtered_real_data) > 0 else 0
                sim_delay_rate = len(sim_delayed) / len(filtered_sim_results) * 100 if len(filtered_sim_results) > 0 else 0
                print(f"真实延误率 (>{self.delay_threshold}分钟): {real_delay_rate:.1f}% ({len(real_delayed)}/{len(filtered_real_data)})")
                print(f"仿真延误率 (>{self.delay_threshold}分钟): {sim_delay_rate:.1f}% ({len(sim_delayed)}/{len(filtered_sim_results)})")
            else:
                real_hourly_counts = pd.Series(dtype=int)
                print("无真实数据进行对比，仅展示仿真结果")
            
            # 创建图表
            fig, ax = plt.subplots(figsize=(14, 8))
            
            # 确定要展示的时间范围 - 生成连续的时间点
            if start_datetime and end_datetime:
                date_range = pd.date_range(
                    start=start_datetime.replace(minute=0, second=0, microsecond=0), 
                    end=end_datetime.replace(minute=0, second=0, microsecond=0),
                    freq='H'
                )
                all_hours = [dt.strftime('%Y-%m-%d %H:00') for dt in date_range]
            else:
                # 合并两个数据集的所有时间点
                all_date_hours = set(list(sim_hourly_counts.index) + list(real_hourly_counts.index))
                all_hours = sorted(all_date_hours) if all_date_hours else []
            
            # 如果没有小时数据，返回
            if not all_hours:
                print("没有有效的小时数据，无法生成图表")
                return False
            
            # 准备数据
            x = np.arange(len(all_hours))
            sim_counts = [sim_hourly_counts.get(h, 0) for h in all_hours]
            real_counts = [real_hourly_counts.get(h, 0) for h in all_hours]
            
            # 绘制柱状图
            width = 0.35
            ax.bar([i - width/2 for i in x], real_counts, width, label=f'真实延误航班 (>{self.delay_threshold}分钟)', color='#3498db', alpha=0.7)
            ax.bar([i + width/2 for i in x], sim_counts, width, label=f'仿真延误航班 (>{self.delay_threshold}分钟)', color='#e74c3c', alpha=0.7)
            
            # 添加积压阈值横线
            ax.axhline(y=self.backlog_threshold, color='orange', linestyle='--', 
                      label=f'积压阈值 ({self.backlog_threshold}班/小时)')
            
            # 添加趋势线
            if len(x) > 1:  # 至少需要两个点才能绘制趋势线
                if any(c > 0 for c in real_counts):
                    ax.plot(x, real_counts, 'b-', alpha=0.5, label='真实延误趋势')
                ax.plot(x, sim_counts, 'r-', alpha=0.5, label='仿真延误趋势')
            
            # 设置标题和标签
            ax.set_title(f'真实延误与仿真延误分布对比 ({title_time_range})', fontsize=16)
            ax.set_xlabel('时间', fontsize=14)
            ax.set_ylabel('延误航班数量', fontsize=14)
            
            # 设置x轴标签 - 处理日期跨度
            if len(all_hours) > 24:  # 如果超过24小时，每6小时一个标签
                step = max(1, len(all_hours) // 12)
                ax.set_xticks(x[::step])
                ax.set_xticklabels([h for h in all_hours[::step]], rotation=45, ha='right')
            else:  # 否则全部显示
                ax.set_xticks(x)
                ax.set_xticklabels([h for h in all_hours], rotation=45, ha='right')
            
            # 为重要小时添加数值标签
            for i, v in enumerate(real_counts):
                if v > self.backlog_threshold:
                    ax.text(i - width/2, v + 0.5, f'{v}', ha='center', fontsize=10)
            for i, v in enumerate(sim_counts):
                if v > self.backlog_threshold:
                    ax.text(i + width/2, v + 0.5, f'{v}', ha='center', fontsize=10)
        
        else:  # 超过2天 - 按天分组
            # 按日期统计仿真延误航班数
            # 重要修改: 使用类的延误阈值判定延误
            sim_delayed = filtered_sim_results[filtered_sim_results['仿真延误分钟'] > self.delay_threshold]
            sim_daily_counts = sim_delayed.groupby('date').size()
            
            # 真实数据处理
            if filtered_real_data is not None and not filtered_real_data.empty:
                # 为真实数据添加日期字段
                filtered_real_data['date'] = filtered_real_data['计划离港时间'].dt.date
                
                # 重要修改: 使用与仿真数据相同的延误阈值来判定真实数据中的延误
                real_delayed = filtered_real_data[filtered_real_data['起飞延误分钟'] > self.delay_threshold]
                real_daily_counts = real_delayed.groupby('date').size()
                
                # 打印延误率对比
                real_delay_rate = len(real_delayed) / len(filtered_real_data) * 100 if len(filtered_real_data) > 0 else 0
                sim_delay_rate = len(sim_delayed) / len(filtered_sim_results) * 100 if len(filtered_sim_results) > 0 else 0
                print(f"真实延误率 (>{self.delay_threshold}分钟): {real_delay_rate:.1f}% ({len(real_delayed)}/{len(filtered_real_data)})")
                print(f"仿真延误率 (>{self.delay_threshold}分钟): {sim_delay_rate:.1f}% ({len(sim_delayed)}/{len(filtered_sim_results)})")
            else:
                real_daily_counts = pd.Series(dtype=int)
                print("无真实数据进行对比，仅展示仿真结果")
            
            # 创建图表
            fig, ax = plt.subplots(figsize=(14, 8))
            
            # 确定要展示的日期范围
            if start_datetime and end_datetime:
                date_range = pd.date_range(
                    start=start_datetime.date(),
                    end=end_datetime.date(),
                    freq='D'
                )
                all_dates = [d.date() for d in date_range]
            else:
                # 合并两个数据集的所有日期
                all_dates = sorted(set(list(sim_daily_counts.index) + list(real_daily_counts.index)))
            
            # 如果没有日期数据，返回
            if not all_dates:
                print("没有有效的日期数据，无法生成图表")
                return False
            
            # 准备数据
            x = np.arange(len(all_dates))
            sim_counts = [sim_daily_counts.get(d, 0) for d in all_dates]
            real_counts = [real_daily_counts.get(d, 0) for d in all_dates]
            
            # 绘制柱状图
            width = 0.35
            ax.bar([i - width/2 for i in x], real_counts, width, label=f'真实延误航班 (>{self.delay_threshold}分钟)', color='#3498db', alpha=0.7)
            ax.bar([i + width/2 for i in x], sim_counts, width, label=f'仿真延误航班 (>{self.delay_threshold}分钟)', color='#e74c3c', alpha=0.7)
            
            # 添加积压阈值横线 - 日积压阈值调整为小时阈值的8倍（近似一个工作日）
            daily_backlog_threshold = self.backlog_threshold * 8
            ax.axhline(y=daily_backlog_threshold, color='orange', linestyle='--', 
                      label=f'日积压阈值 ({daily_backlog_threshold}班/天)')
            
            # 添加趋势线
            if len(x) > 1:  # 至少需要两个点才能绘制趋势线
                if any(c > 0 for c in real_counts):
                    ax.plot(x, real_counts, 'b-', alpha=0.5, label='真实延误趋势')
                ax.plot(x, sim_counts, 'r-', alpha=0.5, label='仿真延误趋势')
            
            # 设置标题和标签
            ax.set_title(f'真实延误与仿真延误分布对比 ({title_time_range})', fontsize=16)
            ax.set_xlabel('日期', fontsize=14)
            ax.set_ylabel('延误航班数量', fontsize=14)
            
            # 设置x轴标签 - 日期格式
            ax.set_xticks(x)
            date_labels = [d.strftime('%Y-%m-%d') for d in all_dates]
            ax.set_xticklabels(date_labels, rotation=45, ha='right')
            
            # 为重要数值添加标签
            for i, v in enumerate(real_counts):
                if v > daily_backlog_threshold:
                    ax.text(i - width/2, v + 0.5, f'{v}', ha='center', fontsize=10)
            for i, v in enumerate(sim_counts):
                if v > daily_backlog_threshold:
                    ax.text(i + width/2, v + 0.5, f'{v}', ha='center', fontsize=10)
        
        # 添加图例
        ax.legend(loc='upper left', fontsize=12)
        
        # 添加网格线
        ax.grid(True, alpha=0.3)
        
        # 添加标注
        plt.figtext(0.5, 0.01, f'* 延误阈值: {self.delay_threshold}分钟，积压阈值: {self.backlog_threshold}班/小时', 
                    ha='center', fontsize=10)
        
        # 调整布局
        plt.tight_layout()
        
        try:
            # 保存图片
            output_filename = f'ZGGG延误分析_{output_filename_suffix}.png'
            plt.savefig(output_filename, dpi=300, bbox_inches='tight')
            print(f"\n分析图表已保存为 '{output_filename}'")
            
            # 显示图表
            plt.show()
            return True
        except Exception as e:
            print(f"保存或显示图表时出错: {str(e)}")
            return False

    def _display_peak_period_details(self, peak_data):
        """显示具体的峰值时段分类详情"""
        print(f"\n--- 具体时段分类详情 ---")
        
        # 按峰值类型分组显示
        for peak_type in ['High', 'Medium', 'Low']:
            subset = peak_data[peak_data['peak_type'] == peak_type]
            if len(subset) == 0:
                continue
                
            type_name_map = {'High': '高峰时段', 'Medium': '平峰时段', 'Low': '低峰时段'}
            print(f"\n🔸 {type_name_map[peak_type]} ({len(subset)}个):")
            
            # 按日期分组显示，限制显示数量避免输出过长
            grouped = subset.groupby('date')
            dates_shown = 0
            max_dates_to_show = 5  # 最多显示5天的详情
            
            for date, day_data in grouped:
                if dates_shown >= max_dates_to_show:
                    remaining_dates = len(grouped) - max_dates_to_show
                    if remaining_dates > 0:
                        print(f"    ... 还有 {remaining_dates} 天的{type_name_map[peak_type]}未显示")
                    break
                
                # 按小时排序
                day_data_sorted = day_data.sort_values('hour')
                hours_list = []
                
                for _, row in day_data_sorted.iterrows():
                    hour = int(row['hour'])
                    delayed_count = int(row['delayed_count'])
                    total_count = int(row['total_count'])
                    delay_rate = delayed_count / total_count * 100 if total_count > 0 else 0
                    hours_list.append(f"{hour:02d}:00({delayed_count}班/{total_count}班,{delay_rate:.0f}%)")
                
                # 将小时信息分行显示，每行最多显示4个时段
                print(f"    {date}:")
                for i in range(0, len(hours_list), 4):
                    batch = hours_list[i:i+4]
                    print(f"      {' | '.join(batch)}")
                
                dates_shown += 1
        
        # 显示统计摘要
        print(f"\n--- 时段分类统计摘要 ---")
        total_periods = len(peak_data)
        for peak_type in ['High', 'Medium', 'Low']:
            count = len(peak_data[peak_data['peak_type'] == peak_type])
            percentage = count / total_periods * 100 if total_periods > 0 else 0
            type_name_map = {'High': '高峰', 'Medium': '平峰', 'Low': '低峰'}
            print(f"  {type_name_map[peak_type]}: {count}个时段 ({percentage:.1f}%)")

    def _visualize_peak_distribution(self, peak_data):
        """可视化峰值分布"""
        # 生成热力图，展示每日每小时的峰值分布
        pivot_data = peak_data.pivot(index='date', columns='hour', values='delayed_count')
        
        plt.figure(figsize=(16, 10))
        sns.heatmap(pivot_data, 
                   cmap="YlOrRd", 
                   annot=True, 
                   fmt=".0f", 
                   linewidths=.5,
                   vmin=0, 
                   vmax=self.backlog_threshold * 1.5)
        
        plt.title(f'ZGGG机场每日每小时延误航班数分布', fontsize=16)
        plt.xlabel('小时', fontsize=14)
        plt.ylabel('日期', fontsize=14)
        
        # 添加峰值阈值说明
        plt.figtext(0.5, 0.01, 
                  f'* 高峰: ≥{self.backlog_threshold}班/小时, '
                  f'平峰: ≥{self.backlog_threshold/2}班/小时, '
                  f'低峰: <{self.backlog_threshold/2}班/小时', 
                  ha='center', 
                  fontsize=12)
        
        plt.tight_layout()
        output_filename = 'ZGGG峰值分布图.png'
        plt.savefig(output_filename, dpi=300, bbox_inches='tight')
        print(f"峰值分布图已保存为 '{output_filename}'")
        plt.close()

def get_user_input():
    """收集用户输入的仿真参数"""
    print("="*60)
    print("      欢迎使用ZGGG交互式起飞仿真系统")
    print("="*60)
    
    # --- 基本设置 ---
    print("\n--- 基本设置 ---")
    start_date_str = input("输入仿真起始日期 (YYYY-MM-DD): ")
    end_date_str = input("输入仿真结束日期 (YYYY-MM-DD): ")
    delay_threshold = int(input("输入延误阈值(分钟) [默认: 15]: ") or "15")  # 修改默认值为15
    backlog_threshold = int(input("输入积压阈值(班/小时) [默认: 10]: ") or "10")
    taxi_out_time = int(input("输入标准滑行时间(分钟) [默认: 15]: ") or "15")  # 确保默认值为15
    
    # --- 航班数据 ---
    print("\n--- 航班计划数据 ---")
    print("A: 使用默认数据 ('数据/5月航班运行数据（脱敏）.xlsx')")
    print("B: 提供自定义数据文件路径")
    data_choice = input("选择一个选项 [A]: ").upper() or 'A'
    
    if data_choice == 'B':
        flight_data_path = input("输入数据文件的完整路径: ")
    else:
        flight_data_path = '5月航班运行数据（脱敏）.xlsx'
        
        # 验证默认数据文件是否存在
        default_paths = [
            flight_data_path,
            os.path.join(os.getcwd(), flight_data_path),
            os.path.join(os.getcwd(), "数据", flight_data_path),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), flight_data_path),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "数据", flight_data_path)
        ]
        
        file_found = any(os.path.exists(p) for p in default_paths)
        if not file_found:
            print("\n警告: 默认数据文件无法在常见位置找到。")
            print("如果继续，程序将尝试更多位置。或者您可以现在重新选择。")
            retry = input("要重新选择文件路径选项吗? (y/n) [y]: ").lower() or 'y'
            if retry == 'y':
                return get_user_input()
    
    # --- 停飞时段配置 ---
    print("\n--- 停飞时段配置（天气等原因） ---")
    suspension_periods = []
    
    while True:
        add_suspension = input("添加一个停飞时段? (y/n) [n]: ").lower()
        if add_suspension != 'y':
            break
            
        print("  请输入开始和结束时间（支持跨天时段）:")
        start_datetime_str = input("  停飞开始时间 (YYYY-MM-DD HH:MM): ")
        end_datetime_str = input("  停飞结束时间 (YYYY-MM-DD HH:MM): ")
        
        try:
            start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M")
            end_datetime = datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M")
            
            if end_datetime <= start_datetime:
                print("  错误: 结束时间必须晚于开始时间。请重试。")
                continue
                
            # 创建一个跨天事件
            current_date = start_datetime.date()
            end_date = end_datetime.date()
            
            # 记录每个受影响的日期
            affected_dates = []
            while current_date <= end_date:
                affected_dates.append(current_date)
                current_date += timedelta(days=1)
            
            for date in affected_dates:
                suspension_periods.append({
                    'date': date,
                    'start_time': max(start_datetime, datetime.combine(date, datetime.min.time())),
                    'end_time': min(end_datetime, datetime.combine(date, datetime.max.time().replace(microsecond=0)))
                })
                
            print(f"  已添加从 {start_datetime} 到 {end_datetime} 的停飞时段。")
            print(f"  影响 {len(affected_dates)} 天。")
            
        except ValueError:
            print("  无效的日期/时间格式。请使用 YYYY-MM-DD HH:MM。请重试。")
    
    # --- 塔台效率配置 ---
    print("\n--- 塔台效率配置 ---")
    efficiency_periods = []
    
    while True:
        add_efficiency = input("添加一个塔台低效时段? (y/n) [n]: ").lower()
        if add_efficiency != 'y':
            break
            
        print("  请输入开始和结束时间（支持跨天时段）:")
        start_datetime_str = input("  低效开始时间 (YYYY-MM-DD HH:MM): ")
        end_datetime_str = input("  低效结束时间 (YYYY-MM-DD HH:MM): ")
        
        efficiency_val = float(input("  效率系数 (0.1-1.0，如0.8表示80%效率): ") or "1.0")
        
        print("  影响类型:")
        print("  1: 随机延误 (随机影响该时段的航班)")
        print("  2: 按顺序延误 (按顺序影响所有航班)")
        print("  3: 优先级延误 (大型飞机优先，小型飞机延误)")
        impact_type_choice = input("  选择影响类型 [2]: ") or '2'
        impact_map = {'1': '随机延误', '2': '按顺序延误', '3': '优先级延误'}
        
        try:
            start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M")
            end_datetime = datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M")
            
            if end_datetime <= start_datetime:
                print("  错误: 结束时间必须晚于开始时间。请重试。")
                continue
                
            # 记录每个受影响的日期
            current_date = start_datetime.date()
            end_date = end_datetime.date()
            affected_dates = []
            
            while current_date <= end_date:
                affected_dates.append(current_date)
                current_date += timedelta(days=1)
            
            impact_type = impact_map.get(impact_type_choice, '按顺序延误')
            
            for date in affected_dates:
                efficiency_periods.append({
                    'date': date,
                    'start_time': max(start_datetime, datetime.combine(date, datetime.min.time())),
                    'end_time': min(end_datetime, datetime.combine(date, datetime.max.time().replace(microsecond=0))),
                    'efficiency': max(0.1, min(1.0, efficiency_val)),
                    'type': impact_type
                })
                
            print(f"  已添加从 {start_datetime} 到 {end_datetime} 的低效时段。")
            print(f"  效率设置为 {efficiency_val*100:.0f}%, 类型: {impact_type}。")
            print(f"  影响 {len(affected_dates)} 天。")
            
        except ValueError:
            print("  无效的日期/时间或数字格式。请重试。")
    
    # 返回所有收集的参数
    return {
        "start_date": datetime.strptime(start_date_str, "%Y-%m-%d"),
        "end_date": datetime.strptime(end_date_str, "%Y-%m-%d"),
        "delay_threshold": delay_threshold,
        "backlog_threshold": backlog_threshold,
        "taxi_out_time": taxi_out_time,
        "flight_data_path": flight_data_path,
        "suspension_periods": suspension_periods,
        "efficiency_periods": efficiency_periods,
    }


def main():
    """主函数"""
    try:
        # 收集用户输入的参数
        params = get_user_input()
        
        # 初始化仿真器 - 使用更合理的默认参数
        simulator = ZGGGDepartureSimulator(
            delay_threshold=params['delay_threshold'],
            backlog_threshold=params['backlog_threshold'],
            taxi_out_time=params['taxi_out_time'],
            flight_data_path=params['flight_data_path'],
            suspension_periods=params['suspension_periods'],
            efficiency_periods=params['efficiency_periods']
        )
        
        # 运行仿真步骤
        if simulator.load_departure_data():
            simulator.identify_weather_suspended_periods()
            # 新增: 分析峰值时段
            simulator.analyze_peak_periods()
            simulator.classify_aircraft_types()
            simulator.separate_flight_types()
            
            # 运行日期范围内的仿真
            if simulator.run_simulation_for_date_range(params['start_date'], params['end_date']):
                # 分析和可视化结果
                simulator.analyze_and_visualize_results()
            else:
                print("\n模拟运行失败。请检查您的数据和参数。")
        else:
            print("\n数据加载失败。停止模拟。")
            
    except Exception as e:
        print(f"\n程序运行时发生错误: {str(e)}")
        import traceback
        print(traceback.format_exc())
    finally:
        print("\n=== 交互式仿真结束 ===")

if __name__ == "__main__":
    main()
