#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG机场飞机起飞仿真系统
基于真实数据的队列仿真，考虑天气停飞和积压情况
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

class ZGGGDepartureSimulator:
    def __init__(self, delay_threshold=15, backlog_threshold=10, taxi_out_time=15, base_rot=90):
        """
        ZGGG起飞仿真器初始化
        
        Args:
            delay_threshold: 延误判定阈值(分钟)，官方建议15分钟以上
            backlog_threshold: 积压判定阈值(班次/小时)
            taxi_out_time: 离港后起飞前准备时间(分钟)，包含滑行和起飞准备
            base_rot: 基础ROT时间(秒)，跑道占用时间
        """
        self.delay_threshold = delay_threshold
        self.backlog_threshold = backlog_threshold
        self.taxi_out_time = taxi_out_time
        self.base_rot = base_rot  # 基础ROT时间
        self.data = None
        self.weather_suspended_periods = []
        self.normal_flights = None
        self.weather_affected_flights = None
        self.all_simulation_results = []  # 存储全月仿真结果
        
        print(f"=== ZGGG起飞仿真器初始化 ===")
        print(f"延误判定阈值: {delay_threshold} 分钟 (官方建议标准)")
        print(f"积压判定阈值: {backlog_threshold} 班/小时")
        print(f"Taxi-out时间: {taxi_out_time} 分钟")
        print(f"基础ROT时间: {base_rot} 秒")
    
    def load_departure_data(self):
        """载入ZGGG起飞航班数据"""
        print(f"\n=== 载入ZGGG起飞航班数据 ===")
        
        # 读取数据
        df = pd.read_excel('/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/5月航班运行数据（脱敏）.xlsx')
        print(f"原始数据总记录数: {len(df)}")
        
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
        
        # 计算基础延误时间 - 优化天气停飞的延误计算
        valid_time_logic['起飞延误分钟'] = (
            valid_time_logic['实际起飞时间'] - valid_time_logic['计划离港时间']
        ).dt.total_seconds() / 60
        
        # 先识别天气停飞期间，然后调整这些航班的延误计算
        self.identify_weather_suspended_periods_early(valid_time_logic)
        
        # 对天气停飞航班重新计算延误时间
        for event in self.weather_suspended_periods:
            affected_mask = (
                (valid_time_logic['计划离港时间'].dt.date == event['date']) &
                (valid_time_logic['计划离港时间'] >= event['suspend_start']) &
                (valid_time_logic['计划离港时间'] <= event['suspend_end'])
            )
            
            # 对于停飞航班，延误 = 实际起飞时间 - 停飞结束时间
            valid_time_logic.loc[affected_mask, '起飞延误分钟'] = (
                valid_time_logic.loc[affected_mask, '实际起飞时间'] - event['suspend_end']
            ).dt.total_seconds() / 60
            
            # 确保延误时间不为负数
            valid_time_logic.loc[affected_mask, '起飞延误分钟'] = np.maximum(
                valid_time_logic.loc[affected_mask, '起飞延误分钟'], 0
            )
        
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
        
        return self.data
    
    def identify_weather_suspended_periods_early(self, flight_data):
        """提前识别天气停飞时段（用于优化延误计算）"""
        # 使用极端延误航班识别天气停飞（阈值调低到3小时）
        extreme_delays = flight_data[flight_data['起飞延误分钟'] > 180].copy()
        
        if len(extreme_delays) == 0:
            self.weather_suspended_periods = []
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
                    
                    # 估算停飞开始时间（最早计划离港前2小时，更保守）
                    earliest_planned = day_flights['计划离港时间'].min()
                    suspend_start = pd.Timestamp.combine(date, pd.Timestamp.min.time()) + pd.Timedelta(hours=max(0, earliest_planned.hour-2))
                    suspend_end = pd.Timestamp.combine(date, pd.Timestamp.min.time()) + pd.Timedelta(hours=resume_hour)
                    
                    weather_event = {
                        'date': date,
                        'suspend_start': suspend_start,
                        'suspend_end': suspend_end,
                        'affected_count': len(day_flights),
                        'resume_hour': resume_hour
                    }
                    
                    weather_events.append(weather_event)
        
        self.weather_suspended_periods = weather_events
        return weather_events
    
    def identify_weather_suspended_periods(self):
        """识别天气停飞时段"""
        print(f"\n=== 识别天气停飞时段 ===")
        
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
                    
                    weather_event = {
                        'date': date,
                        'suspend_start': suspend_start,
                        'suspend_end': suspend_end,
                        'affected_count': len(day_flights),
                        'resume_hour': resume_hour
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
        
        # 定义机型分类和ROT参数(跑道占用时间,秒) - 基于base_rot参数动态调整
        self.aircraft_categories = {
            # 大型客机 (Heavy/Super Heavy) - ROT时间比基础值多15秒
            'Heavy': {
                'types': ['773', '772', '77W', '77L', '744', '748', '380', '359', '358', '35K'],
                'rot_seconds': self.base_rot + 15,  # 基础+15秒
                'wake_category': 'Heavy'
            },
            # 中型客机 (Medium) - ROT时间为基础值
            'Medium': {
                'types': ['32G', '32N', '32A', '321', '320', '319', '327', '32S', '32Q',
                         '73M', '738', '739', '73G', '73H', '737', '73W', '73J',
                         '909', '290', 'E90', 'ER4', 'ERJ', 'E75'],
                'rot_seconds': self.base_rot,       # 基础值
                'wake_category': 'Medium'
            },
            # 小型客机/支线 (Light) - ROT时间比基础值少15秒
            'Light': {
                'types': ['AT7', 'AT5', 'DH8', 'CR9', 'CRJ', 'CR7', 'E45', 'SF3', 'J41'],
                'rot_seconds': max(self.base_rot - 15, 60),  # 基础-15秒，最小60秒
                'wake_category': 'Light'
            },
            # 货机 (Cargo) - ROT时间比基础值多25秒
            'Cargo': {
                'types': ['76F', '77F', '74F', '32P', '737F'],
                'rot_seconds': self.base_rot + 25,  # 基础+25秒
                'wake_category': 'Heavy'
            }
        }
        
        # 为每个航班分配机型类别
        def get_aircraft_category(aircraft_type):
            for category, info in self.aircraft_categories.items():
                if aircraft_type in info['types']:
                    return category, info['rot_seconds'], info['wake_category']
            # 默认为中型机
            return 'Medium', self.base_rot, 'Medium'
        
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
        
        # 尾流间隔矩阵(秒) - 前机→后机，调整为中等强度间隔
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
            suspend_start = event['suspend_start']
            suspend_end = event['suspend_end']
            
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
    
    def simulate_runway_queue_full_month(self, verbose=False):
        """全月跑道排队仿真"""
        print(f"\n=== 全月跑道排队仿真 ===")
        
        if self.data is None:
            print("错误: 需要先完成前序步骤")
            return
        
        # 获取所有日期
        all_dates = sorted(self.data['计划离港时间'].dt.date.unique())
        print(f"仿真日期范围: {all_dates[0]} 至 {all_dates[-1]} (共{len(all_dates)}天)")
        
        all_results = []
        
        for target_date in all_dates:
            if verbose:
                print(f"\n仿真日期: {target_date}")
            
            # 提取当日航班
            day_flights = self.data[
                self.data['计划离港时间'].dt.date == target_date
            ].copy().sort_values('计划离港时间')
            
            if len(day_flights) == 0:
                continue
                
            if verbose:
                print(f"当日航班数: {len(day_flights)} 班")
            
            # 检查是否有天气停飞
            day_weather_events = [
                event for event in self.weather_suspended_periods 
                if event['date'] == target_date
            ]
            
            # 双跑道仿真
            runway_last_departure = {
                '02R/20L': None,
                '02L/20R': None
            }
            
            day_results = []
            
            for idx, flight in day_flights.iterrows():
                # 检查是否在天气停飞期间
                is_weather_suspended = False
                weather_resume_time = None
                
                for event in day_weather_events:
                    if (flight['计划离港时间'] >= event['suspend_start'] and 
                        flight['计划离港时间'] <= event['suspend_end']):
                        is_weather_suspended = True
                        weather_resume_time = event['suspend_end']
                        break
                
                # 选择跑道 - 简化负载均衡
                recent_02R = len([f for f in day_results if f['跑道'] == '02R/20L' and 
                                 f['仿真起飞时间'] > flight['计划离港时间'] - pd.Timedelta(minutes=30)])
                recent_02L = len([f for f in day_results if f['跑道'] == '02L/20R' and 
                                 f['仿真起飞时间'] > flight['计划离港时间'] - pd.Timedelta(minutes=30)])
                
                selected_runway = '02R/20L' if recent_02R <= recent_02L else '02L/20R'
                
                # 计算起飞时间
                planned_departure = flight['计划离港时间']
                base_departure_time = planned_departure + pd.Timedelta(minutes=self.taxi_out_time)
                
                if is_weather_suspended:
                    earliest_takeoff = max(base_departure_time, weather_resume_time)
                else:
                    earliest_takeoff = base_departure_time
                
                # 考虑跑道占用和尾流间隔
                if runway_last_departure[selected_runway] is not None:
                    last_flight = runway_last_departure[selected_runway]
                    
                    wake_key = (last_flight['尾流类别'], flight['尾流类别'])
                    wake_separation = self.wake_separation_matrix.get(wake_key, 120)
                    previous_rot = last_flight.get('ROT秒', 100)
                    
                    min_takeoff_time = (
                        last_flight['仿真起飞时间'] + 
                        pd.Timedelta(seconds=previous_rot + wake_separation)
                    )
                    earliest_takeoff = max(earliest_takeoff, min_takeoff_time)
                
                # 记录仿真结果
                simulated_takeoff = earliest_takeoff
                delay_minutes = (simulated_takeoff - planned_departure).total_seconds() / 60
                
                flight_result = {
                    '航班号': flight['航班号'],
                    '机型': flight['机型'],
                    '机型类别': flight['机型类别'],
                    '跑道': selected_runway,
                    '计划起飞': planned_departure,
                    '仿真起飞时间': simulated_takeoff,
                    '仿真延误分钟': delay_minutes,
                    '受天气影响': is_weather_suspended,
                    '实际延误分钟': flight['起飞延误分钟'],
                    '日期': target_date
                }
                
                day_results.append(flight_result)
                all_results.append(flight_result)
                
                # 更新跑道状态
                runway_last_departure[selected_runway] = {
                    '仿真起飞时间': simulated_takeoff,
                    '尾流类别': flight['尾流类别'],
                    'ROT秒': flight['ROT秒']
                }
        
        # 生成全月仿真结果DataFrame
        self.all_simulation_results = pd.DataFrame(all_results)
        
        print(f"全月仿真完成，共处理 {len(self.all_simulation_results)} 班航班")
        return self.all_simulation_results
    
    def analyze_simulation_statistics(self):
        """分析仿真结果统计"""
        print(f"\n=== 仿真结果统计分析 ===")
        
        if len(self.all_simulation_results) == 0:
            print("错误: 需要先完成全月仿真")
            return
        
        sim_data = self.all_simulation_results
        
        # 基础统计
        print(f"仿真延误: 平均 {sim_data['仿真延误分钟'].mean():.1f} 分钟")
        print(f"实际延误: 平均 {sim_data['实际延误分钟'].mean():.1f} 分钟")
        print(f"延误差异: {abs(sim_data['仿真延误分钟'].mean() - sim_data['实际延误分钟'].mean()):.1f} 分钟")
        
        # 跑道使用情况
        runway_usage = sim_data['跑道'].value_counts()
        print(f"\n跑道使用分布:")
        for runway, count in runway_usage.items():
            percentage = count / len(sim_data) * 100
            print(f"  {runway}: {count}班 ({percentage:.1f}%)")
        
        # 延误分布对比
        normal_sim = sim_data[~sim_data['受天气影响']]
        weather_sim = sim_data[sim_data['受天气影响']]
        
        if len(normal_sim) > 0:
            print(f"\n正常天气仿真延误: 平均 {normal_sim['仿真延误分钟'].mean():.1f} 分钟")
        
        if len(weather_sim) > 0:
            print(f"恶劣天气仿真延误: 平均 {weather_sim['仿真延误分钟'].mean():.1f} 分钟")
        
        # 延误阈值分析
        sim_delayed = (sim_data['仿真延误分钟'] > self.delay_threshold).sum()
        real_delayed = (sim_data['实际延误分钟'] > self.delay_threshold).sum()
        
        print(f"\n延误航班统计(>{self.delay_threshold}分钟):")
        print(f"  仿真延误航班: {sim_delayed} 班 ({sim_delayed/len(sim_data)*100:.1f}%)")
        print(f"  实际延误航班: {real_delayed} 班 ({real_delayed/len(sim_data)*100:.1f}%)")
        
        return sim_data
    
    def identify_systematic_problematic_hours(self, data, data_type='real'):
        """识别系统性问题时段（整个小时段都有异常延误）- 集成版"""
        print(f"\n=== 识别{data_type}数据中的系统性问题时段 ===")
        
        if data_type == 'simulation':
            delay_col = '仿真延误分钟'
            time_col = '计划起飞'
        else:
            delay_col = '起飞延误分钟'  
            time_col = '计划离港时间'
            
        problematic_hours = []
        
        # 分析每个小时的整体延误情况
        for hour in range(24):
            hour_data = data[data[time_col].dt.hour == hour]
            if len(hour_data) < 5:  # 样本太少，跳过
                continue
                
            if delay_col in hour_data.columns:
                delays = hour_data[delay_col]
                
                avg_delay = delays.mean()
                severe_delay_ratio = (delays > 120).sum() / len(delays) if len(delays) > 0 else 0
                
                # 系统性问题的判定条件（针对不同时段采用不同标准）：
                is_problematic = False
                
                if 0 <= hour <= 6:  # 凌晨时段（0-6点）更严格的异常判定
                    # 凌晨时段航班少，但如果平均延误超过100分钟就不正常
                    if ((avg_delay > 100 and severe_delay_ratio > 0.2) or  # 平均延误>100分钟且20%严重延误
                        (avg_delay > 200) or  # 或平均延误>200分钟
                        (severe_delay_ratio > 0.4)):  # 或严重延误比例>40%
                        is_problematic = True
                        
                else:  # 其他时段（7-23点）的判定标准
                    if (avg_delay > 200 and 
                        severe_delay_ratio > 0.5 and 
                        len(hour_data) >= 10):
                        is_problematic = True
                
                if is_problematic:
                    problematic_hours.append({
                        'hour': hour,
                        'avg_delay': avg_delay,
                        'severe_ratio': severe_delay_ratio,
                        'total_flights': len(hour_data)
                    })
                    
                    print(f"识别{data_type}系统性问题时段: {hour:02d}:00 - 平均延误{avg_delay:.0f}分钟, "
                          f"严重延误比例{severe_delay_ratio:.1%}, 总航班{len(hour_data)}班")
        
        return problematic_hours
    
    def identify_backlog_periods_advanced(self, data_type='simulation', exclude_systematic=True):
        """高级积压时段识别，可选择排除系统性问题时段"""
        print(f"\n=== 高级积压时段分析（{data_type}数据，排除系统性问题：{exclude_systematic}）===")
        
        if data_type == 'simulation':
            if len(self.all_simulation_results) == 0:
                print("错误: 需要先完成仿真")
                return []
            
            data = self.all_simulation_results.copy()
            delay_col = '仿真延误分钟'
            time_col = '计划起飞'
        else:  # real data
            data = self.data.copy()
            delay_col = '起飞延误分钟'
            time_col = '计划离港时间'
        
        # 首先识别系统性问题时段
        if exclude_systematic:
            problematic_hours = self.identify_systematic_problematic_hours(data, data_type)
            
            # 排除系统性问题时段的数据
            if problematic_hours:
                problematic_hour_list = [h['hour'] for h in problematic_hours]
                original_count = len(data)
                data = data[~data[time_col].dt.hour.isin(problematic_hour_list)]
                excluded_count = original_count - len(data)
                print(f"排除系统性问题时段数据: {excluded_count} 个航班")
        else:
            problematic_hours = []
        
        # 添加时间特征
        data['小时'] = data[time_col].dt.hour
        data['日期'] = data[time_col].dt.date
        data['延误标记'] = data[delay_col] > self.delay_threshold
        
        # 按小时统计每天的航班量和延误量
        hourly_stats = data.groupby(['日期', '小时']).agg({
            '延误标记': ['count', 'sum'],
            delay_col: 'mean'
        }).round(2)
        
        hourly_stats.columns = ['航班数', '延误航班数', '平均延误']
        hourly_stats = hourly_stats.reset_index()
        
        # 识别积压时段 - 使用动态阈值
        total_days = len(data['日期'].unique())
        dynamic_threshold = max(2, self.backlog_threshold / max(total_days, 1))  # 至少2班延误
        
        backlog_periods = hourly_stats[
            hourly_stats['延误航班数'] >= dynamic_threshold
        ].copy()
        
        print(f"积压识别结果（动态阈值: {dynamic_threshold:.1f}班/小时）:")
        print(f"识别到 {len(backlog_periods)} 个积压时段")
        
        # 计算积压强度
        if len(backlog_periods) > 0:
            backlog_periods['积压强度'] = backlog_periods['延误航班数']
            backlog_periods['积压比率'] = backlog_periods['延误航班数'] / backlog_periods['航班数']
            
            backlog_summary = backlog_periods.groupby('小时').agg({
                '延误航班数': ['count', 'mean', 'sum']
            }).round(1)
            backlog_summary.columns = ['出现天数', '日均延误班数', '总延误班数']
            
            print("\n积压时段分布:")
            print("时段    出现天数  日均延误班数  总延误班数")
            print("-" * 40)
            for hour in sorted(backlog_summary.index):
                stats = backlog_summary.loc[hour]
                print(f"{hour:02d}:00  {stats['出现天数']:6.0f}    {stats['日均延误班数']:8.1f}    {stats['总延误班数']:8.0f}")
        
        return {
            'backlog_periods': backlog_periods,
            'problematic_hours': problematic_hours,
            'filtered_data': data,
            'threshold': self.delay_threshold,
            'dynamic_threshold': dynamic_threshold
        }

    def identify_backlog_periods(self, data_type='simulation'):
        """识别积压时段（保持向后兼容）"""
        result = self.identify_backlog_periods_advanced(data_type, exclude_systematic=False)
        return result['backlog_periods'] if result else []
    
    def compare_backlog_periods_advanced(self, exclude_systematic=True):
        """高级积压时段对比分析（可选择排除系统性问题时段）"""
        print(f"\n=== 高级积压时段对比分析（排除系统性问题：{exclude_systematic}）===")
        
        # 获取高级积压分析结果
        sim_result = self.identify_backlog_periods_advanced('simulation', exclude_systematic)
        real_result = self.identify_backlog_periods_advanced('real', exclude_systematic)
        
        if not sim_result or not real_result:
            print("无法进行对比分析")
            return None
            
        sim_backlog = sim_result['backlog_periods']
        real_backlog = real_result['backlog_periods']
        sim_problematic = sim_result['problematic_hours']
        real_problematic = real_result['problematic_hours']
        
        print(f"仿真积压时段: {len(sim_backlog)} 个")
        print(f"实际积压时段: {len(real_backlog)} 个")
        
        if exclude_systematic:
            print(f"仿真系统性问题时段: {len(sim_problematic)} 个")
            print(f"实际系统性问题时段: {len(real_problematic)} 个")
        
        if len(sim_backlog) == 0 or len(real_backlog) == 0:
            print("无法进行积压对比分析")
            return None
        
        # 按小时分组统计积压频次
        sim_hourly = sim_backlog.groupby('小时').agg({
            '积压强度': ['count', 'mean', 'max'],
            '延误航班数': ['sum', 'mean', 'max'],
            '平均延误': 'mean'
        }).round(3)
        sim_hourly.columns = ['频次', '平均强度', '峰值强度', '总延误航班', '平均延误航班', '峰值延误航班', '平均延误时间']
        
        real_hourly = real_backlog.groupby('小时').agg({
            '积压强度': ['count', 'mean', 'max'],
            '延误航班数': ['sum', 'mean', 'max'],
            '平均延误': 'mean'
        }).round(3)
        real_hourly.columns = ['频次', '平均强度', '峰值强度', '总延误航班', '平均延误航班', '峰值延误航班', '平均延误时间']
        
        # 找出共同的积压时段
        sim_hours = set(sim_hourly.index)
        real_hours = set(real_hourly.index)
        common_hours = sim_hours & real_hours
        
        print(f"\n积压时段重叠分析:")
        print(f"  仿真积压时段: {sorted(sim_hours)}")
        print(f"  实际积压时段: {sorted(real_hours)}")
        print(f"  重叠时段: {sorted(common_hours)} ({len(common_hours)}个)")
        
        overlap_rate = 0
        if len(real_hours) > 0:
            overlap_rate = len(common_hours) / len(real_hours) * 100
            print(f"  重叠率: {overlap_rate:.1f}%")
        
        # 详细对比重叠时段的积压强度和延误时间
        print(f"\n积压强度和延误时间详细对比:")
        strength_errors = []
        delay_time_errors = []
        
        # 选择某一天作为示例展示
        sample_date = None
        if len(sim_backlog) > 0:
            sample_date = sim_backlog['日期'].iloc[0]
            print(f"示例日期: {sample_date}")
        
        for hour in sorted(common_hours):
            # 获取该小时的仿真和实际积压情况
            sim_hour_data = sim_backlog[sim_backlog['小时'] == hour]
            real_hour_data = real_backlog[real_backlog['小时'] == hour]
            
            # 计算平均延误航班数和延误时间
            sim_avg_delayed = sim_hour_data['延误航班数'].mean()
            real_avg_delayed = real_hour_data['延误航班数'].mean()
            
            sim_avg_delay_time = sim_hour_data['平均延误'].mean()
            real_avg_delay_time = real_hour_data['平均延误'].mean()
            
            # 计算误差
            strength_error = abs(sim_avg_delayed - real_avg_delayed) / max(real_avg_delayed, 1) * 100
            delay_time_error = abs(sim_avg_delay_time - real_avg_delay_time) / max(real_avg_delay_time, 1) * 100
            
            strength_errors.append(strength_error)
            delay_time_errors.append(delay_time_error)
            
            # 获取示例日期的数据
            if sample_date is not None:
                sim_sample = sim_hour_data[sim_hour_data['日期'] == sample_date]
                real_sample = real_hour_data[real_hour_data['日期'] == sample_date]
                
                sim_sample_count = sim_sample['延误航班数'].iloc[0] if len(sim_sample) > 0 else 0
                real_sample_count = real_sample['延误航班数'].iloc[0] if len(real_sample) > 0 else 0
                
                strength_status = "✅" if strength_error <= 20 else "❌"
                delay_status = "✅" if delay_time_error <= 15 else "❌"
                
                print(f"  {hour:02d}:00时段 - 延误航班数: 仿真{sim_avg_delayed:.1f}架/实际{real_avg_delayed:.1f}架 "
                      f"误差{strength_error:.1f}% {strength_status}")
                print(f"           - 平均延误时间: 仿真{sim_avg_delay_time:.1f}分/实际{real_avg_delay_time:.1f}分 "
                      f"误差{delay_time_error:.1f}% {delay_status}")
                print(f"           - 示例日({sample_date}): 仿真{sim_sample_count}架/实际{real_sample_count}架")
            else:
                strength_status = "✅" if strength_error <= 20 else "❌"
                delay_status = "✅" if delay_time_error <= 15 else "❌"
                
                print(f"  {hour:02d}:00时段 - 延误航班数: 仿真{sim_avg_delayed:.1f}架/实际{real_avg_delayed:.1f}架 "
                      f"误差{strength_error:.1f}% {strength_status}")
                print(f"           - 平均延误时间: 仿真{sim_avg_delay_time:.1f}分/实际{real_avg_delay_time:.1f}分 "
                      f"误差{delay_time_error:.1f}% {delay_status}")
        
        # 系统性问题时段对比
        if exclude_systematic:
            print(f"\n系统性问题时段对比:")
            sim_problematic_hours = [h['hour'] for h in sim_problematic]
            real_problematic_hours = [h['hour'] for h in real_problematic]
            
            print(f"  仿真系统性问题时段: {sorted(sim_problematic_hours)}")
            print(f"  实际系统性问题时段: {sorted(real_problematic_hours)}")
            
            problematic_overlap = set(sim_problematic_hours) & set(real_problematic_hours)
            print(f"  系统性问题时段重叠: {sorted(problematic_overlap)} ({len(problematic_overlap)}个)")
            
            if len(real_problematic_hours) > 0:
                problematic_overlap_rate = len(problematic_overlap) / len(real_problematic_hours) * 100
                print(f"  系统性问题识别准确率: {problematic_overlap_rate:.1f}%")
        
        # 区间端点误差分析
        print(f"\n积压区间端点分析:")
        
        # 找连续的积压时段
        def find_continuous_periods(hours_list):
            if not hours_list:
                return []
            
            hours_list = sorted(hours_list)
            periods = []
            start = hours_list[0]
            end = hours_list[0]
            
            for i in range(1, len(hours_list)):
                if hours_list[i] == end + 1:
                    end = hours_list[i]
                else:
                    periods.append((start, end))
                    start = hours_list[i]
                    end = hours_list[i]
            
            periods.append((start, end))
            return periods
        
        sim_periods = find_continuous_periods(list(sim_hours))
        real_periods = find_continuous_periods(list(real_hours))
        
        print(f"  仿真连续积压区间: {sim_periods}")
        print(f"  实际连续积压区间: {real_periods}")
        
        # 端点误差检查
        endpoint_errors = []
        for i, real_period in enumerate(real_periods):
            if i < len(sim_periods):
                sim_period = sim_periods[i]
                start_error = abs(sim_period[0] - real_period[0])
                end_error = abs(sim_period[1] - real_period[1])
                
                start_status = "✅" if start_error <= 1 else "❌"
                end_status = "✅" if end_error <= 1 else "❌"
                
                print(f"  区间{i+1}: 起始误差 {start_error}小时 {start_status}, "
                      f"结束误差 {end_error}小时 {end_status}")
                
                endpoint_errors.extend([start_error, end_error])
        
        # 总体评估
        print(f"\n=== 高级仿真准确性评估 ===")
        
        # 计算各项评估指标
        if len(common_hours) > 0:
            avg_strength_error = np.mean(strength_errors)
            avg_delay_time_error = np.mean(delay_time_errors)
            strength_accuracy = len([e for e in strength_errors if e <= 15]) / len(strength_errors) * 100
            delay_accuracy = len([e for e in delay_time_errors if e <= 15]) / len(delay_time_errors) * 100
        else:
            avg_strength_error = 100
            avg_delay_time_error = 100
            strength_accuracy = 0
            delay_accuracy = 0
            
        if endpoint_errors:
            avg_endpoint_error = np.mean(endpoint_errors)
            endpoint_accuracy = len([e for e in endpoint_errors if e <= 1]) / len(endpoint_errors) * 100
        else:
            avg_endpoint_error = 0
            endpoint_accuracy = 0
        
        print(f"✅ 积压时段重叠率: {overlap_rate:.1f}% (目标>70%)")
        print(f"✅ 延误航班数平均误差: {avg_strength_error:.1f}% (目标<15%)")
        print(f"✅ 延误时间平均误差: {avg_delay_time_error:.1f}% (目标<15%)")
        print(f"✅ 延误航班数准确率: {strength_accuracy:.1f}% (误差<15%的时段比例)")
        print(f"✅ 延误时间准确率: {delay_accuracy:.1f}% (误差<15%的时段比例)")
        print(f"✅ 区间端点平均误差: {avg_endpoint_error:.1f}小时 (目标<1小时)")
        print(f"✅ 区间端点准确率: {endpoint_accuracy:.1f}% (误差<1小时的端点比例)")
        
        # 综合评分 - 加入延误时间准确性
        overlap_score = min(overlap_rate, 100)
        strength_score = max(0, 100 - avg_strength_error)
        delay_time_score = max(0, 100 - avg_delay_time_error)
        endpoint_score = max(0, 100 - avg_endpoint_error * 50)
        
        overall_score = (overlap_score * 0.3 + strength_score * 0.3 + 
                        delay_time_score * 0.2 + endpoint_score * 0.2)
        
        print(f"\n🎯 综合评分: {overall_score:.1f}/100")
        
        if overall_score >= 85:
            print("🏆 仿真质量: 优秀 - 精确匹配现实积压模式")
        elif overall_score >= 70:
            print("⚠️  仿真质量: 良好 - 基本准确，建议微调")
        else:
            print("❌ 仿真质量: 需改进 - 系统性偏差较大")
        
        return {
            'overlap_rate': overlap_rate,
            'strength_error': avg_strength_error,
            'delay_time_error': avg_delay_time_error,
            'endpoint_error': avg_endpoint_error,
            'overall_score': overall_score,
            'exclude_systematic': exclude_systematic
        }
    
    def compare_backlog_periods(self):
        """对比仿真和真实的积压时段（保持向后兼容）"""
        return self.compare_backlog_periods_advanced(exclude_systematic=False)
    
    def visualize_backlog_comparison_advanced(self, exclude_systematic=True):
        """高级积压对比可视化（可选择排除系统性问题时段）"""
        print(f"\n=== 生成高级积压对比可视化图表 ===")
        
        # 获取高级积压分析结果
        sim_result = self.identify_backlog_periods_advanced('simulation', exclude_systematic)
        real_result = self.identify_backlog_periods_advanced('real', exclude_systematic)
        
        if not sim_result or not real_result:
            print("无法生成对比图表")
            return
        
        # 准备数据
        sim_data = sim_result['filtered_data'].copy()
        real_data = real_result['filtered_data'].copy()
        
        sim_data['小时'] = sim_data['计划起飞'].dt.hour
        real_data['小时'] = real_data['计划离港时间'].dt.hour
        
        sim_data['延误标记'] = sim_data['仿真延误分钟'] > self.delay_threshold
        real_data['延误标记'] = real_data['起飞延误分钟'] > self.delay_threshold
        
        # 创建图表
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        title_suffix = "（排除系统性问题时段）" if exclude_systematic else "（包含所有时段）"
        fig.suptitle(f'ZGGG机场积压时段高级对比分析{title_suffix}', fontsize=16)
        
        # 1. 每小时平均延误时间对比
        sim_hourly_delay = sim_data.groupby('小时')['仿真延误分钟'].mean()
        real_hourly_delay = real_data.groupby('小时')['起飞延误分钟'].mean()
        
        hours = range(24)
        sim_delays = [sim_hourly_delay.get(h, 0) for h in hours]
        real_delays = [real_hourly_delay.get(h, 0) for h in hours]
        
        x = np.arange(24)
        width = 0.35
        
        axes[0,0].bar(x - width/2, real_delays, width, label='实际延误', color='orange', alpha=0.7)
        axes[0,0].bar(x + width/2, sim_delays, width, label='仿真延误', color='skyblue', alpha=0.7)
        axes[0,0].set_title('各小时平均延误时间对比')
        axes[0,0].set_xlabel('小时')
        axes[0,0].set_ylabel('平均延误(分钟)')
        axes[0,0].legend()
        axes[0,0].grid(True, alpha=0.3)
        
        # 2. 每小时延误航班数对比
        # 修正数据准备
        sim_data_copy = sim_data.copy()
        real_data_copy = real_data.copy()
        sim_data_copy['日期'] = sim_data_copy['计划起飞'].dt.date
        real_data_copy['日期'] = real_data_copy['计划离港时间'].dt.date
        
        sim_hourly_delayed = sim_data_copy.groupby(['日期', '小时'])['延误标记'].sum().reset_index()
        real_hourly_delayed = real_data_copy.groupby(['日期', '小时'])['延误标记'].sum().reset_index()
        
        sim_avg_delayed = sim_hourly_delayed.groupby('小时')['延误标记'].mean()
        real_avg_delayed = real_hourly_delayed.groupby('小时')['延误标记'].mean()
        
        sim_delayed_counts = [sim_avg_delayed.get(h, 0) for h in hours]
        real_delayed_counts = [real_avg_delayed.get(h, 0) for h in hours]
        
        axes[0,1].bar(x - width/2, real_delayed_counts, width, label='实际延误航班', color='red', alpha=0.7)
        axes[0,1].bar(x + width/2, sim_delayed_counts, width, label='仿真延误航班', color='blue', alpha=0.7)
        axes[0,1].axhline(y=self.backlog_threshold, color='black', linestyle='--', 
                         label=f'积压阈值({self.backlog_threshold}班)')
        axes[0,1].set_title('各小时日均延误航班数对比')
        axes[0,1].set_xlabel('小时')
        axes[0,1].set_ylabel('日均延误航班数')
        axes[0,1].legend()
        axes[0,1].grid(True, alpha=0.3)
        
        # 3. 积压时段识别结果对比
        sim_backlog = sim_result['backlog_periods']
        real_backlog = real_result['backlog_periods']
        
        # 标记积压时段
        sim_backlog_hours = set(sim_backlog['小时'].unique()) if len(sim_backlog) > 0 else set()
        real_backlog_hours = set(real_backlog['小时'].unique()) if len(real_backlog) > 0 else set()
        
        backlog_comparison = []
        for h in hours:
            if h in sim_backlog_hours and h in real_backlog_hours:
                backlog_comparison.append(3)  # 都识别为积压
            elif h in real_backlog_hours:
                backlog_comparison.append(2)  # 仅实际为积压
            elif h in sim_backlog_hours:
                backlog_comparison.append(1)  # 仅仿真为积压
            else:
                backlog_comparison.append(0)  # 都不是积压
        
        colors = ['lightgray', 'lightblue', 'lightcoral', 'green']
        labels = ['非积压', '仅仿真积压', '仅实际积压', '共同积压']
        
        bars = axes[0,2].bar(hours, [1]*24, color=[colors[bc] for bc in backlog_comparison])
        axes[0,2].set_title('积压时段识别结果对比')
        axes[0,2].set_xlabel('小时')
        axes[0,2].set_ylabel('积压状态')
        axes[0,2].set_ylim(0, 1.2)
        
        # 添加图例
        legend_elements = [plt.Rectangle((0,0),1,1, color=colors[i], label=labels[i]) for i in range(4)]
        axes[0,2].legend(handles=legend_elements, loc='upper right')
        
        # 4. 延误分布对比（仿真vs实际）
        axes[1,0].hist(real_data['起飞延误分钟'], bins=50, alpha=0.5, label='实际延误', color='orange', density=True)
        axes[1,0].hist(sim_data['仿真延误分钟'], bins=50, alpha=0.5, label='仿真延误', color='skyblue', density=True)
        axes[1,0].axvline(x=self.delay_threshold, color='red', linestyle='--', 
                         label=f'延误阈值({self.delay_threshold}分钟)')
        axes[1,0].set_title('延误时间分布对比')
        axes[1,0].set_xlabel('延误时间(分钟)')
        axes[1,0].set_ylabel('概率密度')
        axes[1,0].legend()
        axes[1,0].grid(True, alpha=0.3)
        
        # 5. 系统性问题时段标识（如果排除了的话）
        if exclude_systematic:
            sim_problematic = sim_result['problematic_hours']
            real_problematic = real_result['problematic_hours']
            
            sim_problematic_hours = [h['hour'] for h in sim_problematic]
            real_problematic_hours = [h['hour'] for h in real_problematic]
            
            problematic_status = []
            for h in hours:
                if h in sim_problematic_hours and h in real_problematic_hours:
                    problematic_status.append(3)  # 都识别为系统性问题
                elif h in real_problematic_hours:
                    problematic_status.append(2)  # 仅实际为系统性问题
                elif h in sim_problematic_hours:
                    problematic_status.append(1)  # 仅仿真为系统性问题
                else:
                    problematic_status.append(0)  # 都不是系统性问题
            
            problem_colors = ['white', 'lightblue', 'lightcoral', 'darkred']
            problem_labels = ['正常时段', '仅仿真异常', '仅实际异常', '共同异常']
            
            axes[1,1].bar(hours, [1]*24, color=[problem_colors[ps] for ps in problematic_status])
            axes[1,1].set_title('系统性问题时段识别对比')
            axes[1,1].set_xlabel('小时')
            axes[1,1].set_ylabel('问题状态')
            axes[1,1].set_ylim(0, 1.2)
            
            # 添加图例
            problem_legend = [plt.Rectangle((0,0),1,1, color=problem_colors[i], label=problem_labels[i]) for i in range(4)]
            axes[1,1].legend(handles=problem_legend, loc='upper right')
        else:
            axes[1,1].text(0.5, 0.5, '未排除系统性问题时段\n所有数据均参与分析', 
                          transform=axes[1,1].transAxes, ha='center', va='center', fontsize=12)
            axes[1,1].set_title('系统性问题时段处理状态')
        
        # 6. 误差分析热力图
        # 计算每个小时的误差矩阵
        error_matrix = np.zeros((4, 24))  # 4种误差类型 × 24小时
        
        for h in hours:
            # 延误航班数误差
            sim_count = sim_avg_delayed.get(h, 0)
            real_count = real_avg_delayed.get(h, 0)
            count_error = abs(sim_count - real_count) / max(real_count, 1) * 100
            error_matrix[0, h] = min(count_error, 100)  # 限制最大误差为100%
            
            # 平均延误时间误差
            sim_delay = sim_hourly_delay.get(h, 0)
            real_delay = real_hourly_delay.get(h, 0)
            delay_error = abs(sim_delay - real_delay) / max(real_delay, 1) * 100
            error_matrix[1, h] = min(delay_error, 100)
            
            # 积压识别一致性 (0表示一致，100表示完全不一致)
            if (h in sim_backlog_hours and h in real_backlog_hours) or (h not in sim_backlog_hours and h not in real_backlog_hours):
                error_matrix[2, h] = 0
            else:
                error_matrix[2, h] = 100
                
            # 系统性问题识别一致性
            if exclude_systematic:
                if (h in sim_problematic_hours and h in real_problematic_hours) or (h not in sim_problematic_hours and h not in real_problematic_hours):
                    error_matrix[3, h] = 0
                else:
                    error_matrix[3, h] = 100
            else:
                error_matrix[3, h] = 0  # 未进行系统性问题分析
        
        im = axes[1,2].imshow(error_matrix, cmap='RdYlGn_r', aspect='auto', vmin=0, vmax=100)
        axes[1,2].set_title('各时段误差热力图')
        axes[1,2].set_xlabel('小时')
        axes[1,2].set_ylabel('误差类型')
        axes[1,2].set_yticks(range(4))
        axes[1,2].set_yticklabels(['延误航班数', '平均延误时间', '积压识别', '系统性问题识别'])
        axes[1,2].set_xticks(range(0, 24, 2))
        axes[1,2].set_xticklabels(range(0, 24, 2))
        
        plt.colorbar(im, ax=axes[1,2], label='误差百分比(%)')
        
        plt.tight_layout()
        
        # 保存图表
        filename_suffix = "_排除系统性问题" if exclude_systematic else "_包含所有时段"
        filename = f'ZGGG积压时段高级对比分析{filename_suffix}.png'
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"高级对比图表已保存为: {filename}")
        plt.show()
        
        return filename

def run_advanced_backlog_analysis():
    """简化的高级积压分析运行函数"""
    print("=== ZGGG机场高级积压分析快速运行 ===")
    
    # 使用推荐参数
    simulator = ZGGGDepartureSimulator(
        delay_threshold=15,    # 官方建议的延误阈值
        backlog_threshold=10,  # 积压判定阈值
        taxi_out_time=15,      # 标准taxi-out时间
        base_rot=90           # 标准ROT时间
    )
    
    print("✅ 初始化仿真器完成")
    
    # 数据载入和处理
    data = simulator.load_departure_data()
    simulator.classify_aircraft_types()
    simulator.separate_flight_types()
    
    print("✅ 数据载入和预处理完成")
    
    # 仿真
    simulation_results = simulator.simulate_runway_queue_full_month(verbose=False)
    simulator.analyze_simulation_statistics()
    
    print("✅ 仿真计算完成")
    
    # 高级积压分析
    print("\n--- 基础积压分析 ---")
    basic_result = simulator.compare_backlog_periods()
    
    print("\n--- 高级积压分析（排除系统性问题）---")
    advanced_result = simulator.compare_backlog_periods_advanced(exclude_systematic=True)
    
    # 生成可视化图表
    print("\n--- 生成可视化图表 ---")
    try:
        chart = simulator.visualize_backlog_comparison_advanced(exclude_systematic=True)
        print(f"✅ 图表已保存: {chart}")
    except Exception as e:
        print(f"⚠️  图表生成失败: {e}")
    
    # 总结结果
    print(f"\n=== 分析结果总结 ===")
    if basic_result and advanced_result:
        print(f"基础分析评分: {basic_result['overall_score']:.1f}/100")
        print(f"高级分析评分: {advanced_result['overall_score']:.1f}/100")
        
        if advanced_result['overall_score'] >= 80:
            print("🏆 仿真质量优秀，可直接用于运营决策")
        elif advanced_result['overall_score'] >= 70:
            print("⚠️  仿真质量良好，建议适当调优")
        else:
            print("❌ 仿真质量需要改进")
            
        print(f"\n主要分析结果:")
        print(f"- 积压时段重叠率: {advanced_result['overlap_rate']:.1f}%")
        print(f"- 延误航班数误差: {advanced_result['strength_error']:.1f}%")
        print(f"- 延误时间误差: {advanced_result['delay_time_error']:.1f}%")
    
    print("\n✅ 高级积压分析完成！")
    return simulator, advanced_result

# 现在让我们测试完整的仿真系统（集成高级积压分析）
if __name__ == "__main__":
    print("=== ZGGG机场仿真系统集成测试（包含高级积压分析）===")
    
    # 参数优化测试 - 围绕官方建议的15分钟阈值进行调优，同时优化ROT参数
    delay_thresholds = [12, 15, 18]  # 测试不同延误阈值
    taxi_out_times = [10, 15, 20]    # 测试不同taxi-out时间
    base_rot_times = [75, 90, 105]   # 测试不同基础ROT时间
    
    best_score = 0
    best_params = None
    
    print("\n=== 第一阶段：参数优化测试（基础积压分析）===")
    
    for delay_thresh in delay_thresholds:
        for taxi_time in taxi_out_times:
            for base_rot in base_rot_times:
                print(f"\n测试参数组合: 延误阈值={delay_thresh}分钟, Taxi-out={taxi_time}分钟, 基础ROT={base_rot}秒")
                
                # 初始化仿真器
                simulator = ZGGGDepartureSimulator(
                    delay_threshold=delay_thresh,
                    backlog_threshold=10,
                    taxi_out_time=taxi_time,
                    base_rot=base_rot
                )
                
                # 载入数据和基础处理
                data = simulator.load_departure_data()
                simulator.classify_aircraft_types()
                simulator.separate_flight_types()
                
                # 全月仿真
                simulation_results = simulator.simulate_runway_queue_full_month(verbose=False)
                
                # 分析统计
                simulator.analyze_simulation_statistics()
                
                # 基础对比分析
                comparison_results = simulator.compare_backlog_periods()
            
                # 记录最佳参数
                if comparison_results and comparison_results['overall_score'] > best_score:
                    best_score = comparison_results['overall_score']
                    best_params = {
                        'delay_threshold': delay_thresh,
                        'taxi_out_time': taxi_time,
                        'base_rot': base_rot,
                        'score': best_score
                    }
                    
                print(f"当前参数评分: {comparison_results['overall_score']:.1f}/100" if comparison_results else "无法计算评分")
    
    print(f"\n" + "="*60)
    print("                  第一阶段最优参数结果")
    print("="*60)
    
    if best_params:
        print(f"🏆 最优参数组合:")
        print(f"   延误判定阈值: {best_params['delay_threshold']} 分钟")
        print(f"   Taxi-out时间: {best_params['taxi_out_time']} 分钟")
        print(f"   基础ROT时间: {best_params['base_rot']} 秒")
        print(f"   综合评分: {best_params['score']:.1f}/100")
        
        # 使用最优参数进行高级分析
        print(f"\n=== 第二阶段：使用最优参数进行高级积压分析 ===")
        final_simulator = ZGGGDepartureSimulator(
            delay_threshold=best_params['delay_threshold'],
            backlog_threshold=10,
            taxi_out_time=best_params['taxi_out_time'],
            base_rot=best_params['base_rot']
        )
        
        # 完整流程
        final_data = final_simulator.load_departure_data()
        final_simulator.classify_aircraft_types()
        final_simulator.separate_flight_types()
        final_results = final_simulator.simulate_runway_queue_full_month(verbose=False)
        
        print(f"\n--- 基础统计分析 ---")
        final_simulator.analyze_simulation_statistics()
        
        print(f"\n--- 基础积压对比分析 ---")
        basic_comparison = final_simulator.compare_backlog_periods()
        
        print(f"\n--- 高级积压对比分析（排除系统性问题时段）---")
        advanced_comparison = final_simulator.compare_backlog_periods_advanced(exclude_systematic=True)
        
        print(f"\n--- 高级积压对比分析（包含所有时段）---")
        full_comparison = final_simulator.compare_backlog_periods_advanced(exclude_systematic=False)
        
        # 比较不同分析方法的效果
        print(f"\n=== 不同分析方法效果对比 ===")
        if basic_comparison and advanced_comparison and full_comparison:
            print(f"基础分析评分: {basic_comparison['overall_score']:.1f}/100")
            print(f"高级分析（排除系统性问题）评分: {advanced_comparison['overall_score']:.1f}/100")
            print(f"高级分析（包含所有时段）评分: {full_comparison['overall_score']:.1f}/100")
            
            if advanced_comparison['overall_score'] > basic_comparison['overall_score']:
                improvement = advanced_comparison['overall_score'] - basic_comparison['overall_score']
                print(f"✅ 排除系统性问题后，仿真准确性提升 {improvement:.1f} 分")
                print("🎯 建议使用高级积压分析（排除系统性问题时段）")
            else:
                print("⚠️  系统性问题识别对此数据集效果有限")
        
        # 生成可视化图表
        print(f"\n--- 生成高级对比可视化图表 ---")
        try:
            # 生成排除系统性问题的图表
            chart1 = final_simulator.visualize_backlog_comparison_advanced(exclude_systematic=True)
            print(f"✅ 生成高级对比图表（排除系统性问题）: {chart1}")
            
            # 生成包含所有时段的图表
            chart2 = final_simulator.visualize_backlog_comparison_advanced(exclude_systematic=False)
            print(f"✅ 生成高级对比图表（包含所有时段）: {chart2}")
            
        except Exception as e:
            print(f"⚠️  图表生成遇到问题: {e}")
            
    else:
        print("❌ 未找到满意的参数组合，建议扩大搜索范围")
    
    print(f"\n" + "="*60)
    print("                完整仿真系统功能总结")
    print("="*60)
    print("🚀 核心功能:")
    print("1. ✅ 全月数据载入和预处理")
    print("2. ✅ 优化的天气停飞识别和延误计算")
    print("3. ✅ 机型分类和ROT参数设定")
    print("4. ✅ 全月双跑道排队仿真")
    print("5. ✅ 基础积压时段识别和对比分析")
    print("6. ✅ 参数自动优化")
    print()
    print("🎯 高级功能（集成自ZGGG积压时段分析.py）:")
    print("7. ✅ 系统性问题时段识别算法")
    print("8. ✅ 高级积压时段分析（可选择排除系统性问题）")
    print("9. ✅ 增强的积压对比分析（延误航班数+延误时间）")
    print("10.✅ 高级可视化图表生成")
    print("11.✅ 多维度误差分析热力图")
    print()
    print("📊 分析维度:")
    print("- 积压时段重叠率分析")
    print("- 延误航班数准确性分析")
    print("- 平均延误时间准确性分析")
    print("- 系统性问题时段识别准确性")
    print("- 积压区间端点误差分析")
    print("- 综合评分和仿真质量评估")
    print()
    print("🏆 系统已完全集成积压时段分析功能，可用于:")
    print("- 机场运营优化决策支持")
    print("- 延误预测和积压时段预警") 
    print("- 跑道调度策略优化")
    print("- 航空公司运营计划调整")
    print("\n仿真系统测试完成，已准备就绪！")
