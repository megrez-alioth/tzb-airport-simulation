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
    def __init__(self, delay_threshold=8, backlog_threshold=10, taxi_out_time=15):
        """
        ZGGG起飞仿真器初始化
        
        Args:
            delay_threshold: 延误判定阈值(分钟)，可调整参数
            backlog_threshold: 积压判定阈值(班次/小时)
            taxi_out_time: 离港后起飞前准备时间(分钟)，包含滑行和起飞准备
        """
        self.delay_threshold = delay_threshold
        self.backlog_threshold = backlog_threshold
        self.taxi_out_time = taxi_out_time
        self.data = None
        self.weather_suspended_periods = []
        self.normal_flights = None
        self.weather_affected_flights = None
        
        print(f"=== ZGGG起飞仿真器初始化 ===")
        print(f"延误判定阈值: {delay_threshold} 分钟")
        print(f"积压判定阈值: {backlog_threshold} 班/小时")
        print(f"Taxi-out时间: {taxi_out_time} 分钟")
    
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
        
        return self.data
    
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
            return 'Medium', 75, 'Medium'
        
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
    
    def simulate_runway_queue(self, target_date=None, verbose=False):
        """仿真跑道排队"""
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
        
        if day_weather_events:
            print(f"当日天气停飞事件: {len(day_weather_events)} 个")
            for event in day_weather_events:
                print(f"  停飞时段: {event['suspend_start'].strftime('%H:%M')}-{event['suspend_end'].strftime('%H:%M')}")
        
        # 双跑道仿真
        runway_queues = {
            '02R/20L': [],
            '02L/20R': []
        }
        
        runway_last_departure = {
            '02R/20L': None,
            '02L/20R': None
        }
        
        results = []
        
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
            
            # 选择跑道 - 考虑负载均衡和随机性
            runway_loads = {
                rwy: len([f for f in runway_queues[rwy] if f['仿真起飞时间'] > simulated_takeoff - pd.Timedelta(minutes=30)])
                for rwy in runway_queues.keys()
            }
            
            # 80%概率选择负载较轻的跑道，20%概率随机选择（模拟实际调度的灵活性）
            if np.random.random() < 0.8:
                selected_runway = min(runway_loads, key=runway_loads.get)
            else:
                selected_runway = np.random.choice(list(runway_loads.keys()))
            
            # 计算起飞时间 - 考虑taxi-out时间
            planned_departure = flight['计划离港时间']
            # 实际离港时间 = 计划离港时间 + 可能的离港延误
            # 起飞时间 = 实际离港时间 + taxi-out时间 + 可能的跑道等待
            
            # 基础离港时间（假设有一定离港延误）
            base_departure_time = planned_departure + pd.Timedelta(minutes=self.taxi_out_time)
            
            if is_weather_suspended:
                # 天气停飞期间，起飞时间不早于恢复时间
                earliest_takeoff = max(base_departure_time, weather_resume_time)
            else:
                earliest_takeoff = base_departure_time
            
            # 考虑跑道占用和尾流间隔
            if runway_last_departure[selected_runway] is not None:
                last_flight = runway_last_departure[selected_runway]
                
                # 计算尾流间隔
                wake_key = (last_flight['尾流类别'], flight['尾流类别'])
                wake_separation = self.wake_separation_matrix.get(wake_key, 120)
                
                # 考虑前机的ROT时间
                previous_rot = last_flight.get('ROT秒', 100)
                
                # 最早起飞时间 = max(基础起飞时间, 前机起飞时间 + ROT + 尾流间隔)
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
                '实际延误分钟': flight['起飞延误分钟']
            }
            
            results.append(flight_result)
            
            # 更新跑道状态
            runway_last_departure[selected_runway] = {
                '仿真起飞时间': simulated_takeoff,
                '尾流类别': flight['尾流类别'],
                'ROT秒': flight['ROT秒']
            }
            
            if verbose and len(results) <= 10:
                print(f"  {flight['航班号']} ({flight['机型']}) "
                      f"计划{planned_departure.strftime('%H:%M')} → "
                      f"仿真{simulated_takeoff.strftime('%H:%M')} "
                      f"延误{delay_minutes:.0f}分钟 [{selected_runway}]")
        
        # 生成仿真结果DataFrame
        simulation_results = pd.DataFrame(results)
        
        # 统计分析
        print(f"\n=== 仿真结果统计 ===")
        print(f"仿真延误: 平均 {simulation_results['仿真延误分钟'].mean():.1f} 分钟")
        print(f"实际延误: 平均 {simulation_results['实际延误分钟'].mean():.1f} 分钟")
        
        # 跑道使用情况
        runway_usage = simulation_results['跑道'].value_counts()
        print(f"跑道使用分布:")
        for runway, count in runway_usage.items():
            percentage = count / len(simulation_results) * 100
            print(f"  {runway}: {count}班 ({percentage:.1f}%)")
        
        # 延误对比
        normal_sim = simulation_results[~simulation_results['受天气影响']]
        weather_sim = simulation_results[simulation_results['受天气影响']]
        
        if len(normal_sim) > 0:
            print(f"正常天气仿真延误: 平均 {normal_sim['仿真延误分钟'].mean():.1f} 分钟")
        
        if len(weather_sim) > 0:
            print(f"恶劣天气仿真延误: 平均 {weather_sim['仿真延误分钟'].mean():.1f} 分钟")
        
        return simulation_results

# 现在让我们测试完整的仿真系统
if __name__ == "__main__":
    # 初始化仿真器 - 调整参数平衡仿真准确性
    simulator = ZGGGDepartureSimulator(
        delay_threshold=8,      # 延误阈值调整到8分钟
        backlog_threshold=10,   # 积压阈值保持10班/小时
        taxi_out_time=15        # taxi-out时间调整到15分钟
    )
    
    # 第一步：载入数据
    data = simulator.load_departure_data()
    
    # 第二步：识别天气停飞时段
    weather_events = simulator.identify_weather_suspended_periods()
    
    print(f"\n=== 第一阶段完成 ===")
    print(f"仿真数据载入完成，共 {len(data)} 班航班(含估算起飞时间)")
    print(f"分析数据: {len(simulator.analysis_data)} 班航班(真实起飞时间)")
    print(f"识别出 {len(weather_events)} 个天气停飞事件")
    print(f"ZGGG双跑道配置: 02R/20L, 02L/20R")
    
    # 第三步：机型分类
    print(f"\n" + "="*50)
    simulator.classify_aircraft_types()
    
    # 第四步：分离航班类型
    simulator.separate_flight_types()
    
    # 第五步：跑道排队仿真 (选择一个典型日期)
    print(f"\n" + "="*50)
    simulation_results = simulator.simulate_runway_queue(verbose=True)
    
    print(f"\n=== 仿真系统测试完成 ===")
    print("系统具备以下功能:")
    print("1. ✅ 数据载入和预处理 (含缺失数据估算)")
    print("2. ✅ 天气停飞事件识别")
    print("3. ✅ 机型分类和ROT参数设定")
    print("4. ✅ 航班类型分离 (正常/天气影响)")
    print("5. ✅ 双跑道排队仿真 (含尾流间隔)")
    print("\n可以进行不同日期的仿真测试和参数调优。")
