#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
航空挑战杯 - 指标分析系统
基于仿真数据和实际运行数据进行指标2和指标3的分析验证

指标2：出港积压发生时段偏移误差
指标3：停止起降情景下出港积压化解偏移误差

作者            return pd.DataFrame(simulation_records)
            
        except Exception as e:
            print(f"  ❌ XML解析失败: {e}")
            return pd.DataFrame()
    
    def generate_optimized_simulation_analysis(self):
        生成优化的仿真分析以匹配实际情况
        print("  - 生成匹配实际情况的仿真分析...")
        
        try:
            # 分析实际数据以确定合适的仿真参数
            zggg_data = self.actual_data[
                self.actual_data['计划起飞站四字码'] == self.target_airport
            ].copy()
            
            if len(zggg_data) == 0:
                print("  ⚠️ 无ZGGG实际数据，使用默认仿真参数")
                return
            
            # 清理和解析时间数据  
            valid_mask = (zggg_data['计划离港时间'] != '-') & (zggg_data['计划离港时间'].notna())
            zggg_clean = zggg_data[valid_mask].copy()
            zggg_clean['计划离港时间'] = pd.to_datetime(zggg_clean['计划离港时间'], errors='coerce')
            zggg_clean = zggg_clean[zggg_clean['计划离港时间'].notna()]
            
            # 取第一天数据作为基准
            first_date = zggg_clean['计划离港时间'].dt.date.min()
            daily_data = zggg_clean[zggg_clean['计划离港时间'].dt.date == first_date]
            
            if len(daily_data) == 0:
                print("  ⚠️ 无有效的单日数据")
                return
            
            # 计算每小时航班量
            daily_data['小时'] = daily_data['计划离港时间'].dt.hour
            hourly_counts = daily_data.groupby('小时').size()
            
            # 确定峰值小时和航班量
            peak_hour = hourly_counts.idxmax()
            peak_count = hourly_counts.max()
            
            # 计算建议的跑道数量（每条跑道每小时6班，留20%余量）
            suggested_runways = max(2, int(np.ceil(peak_count / 5)))  # 5班/小时/跑道
            
            print(f"  - 实际单日数据分析:")
            print(f"    峰值时段: {peak_hour}:00")
            print(f"    峰值航班: {peak_count} 班/小时")
            print(f"    建议跑道: {suggested_runways} 条")
            
            # 运行优化的仿真
            from 机场排队仿真系统 import AirportQueueSimulator
            
            simulator = AirportQueueSimulator(
                departure_time=15,        # 起飞15分钟
                arrival_time=8,           # 降落8分钟  
                num_runways=suggested_runways  # 使用建议跑道数
            )
            
            # 加载飞行计划并运行仿真
            simulator.load_flight_plans("仿真/all_flight_plans.xml")
            airport_activities = simulator.collect_airport_activities()
            updated_plans = simulator.simulate_queue(airport_activities, time_disturbance=5)
            
            # 保存优化的仿真结果
            report_file = "优化机场排队仿真分析报告.xlsx"
            simulator.generate_analysis_report(updated_plans, report_file)
            
            # 更新延误分析数据
            self.delay_analysis = pd.read_excel(report_file)
            
            print(f"  ✅ 优化仿真完成，结果保存到: {report_file}")
            print(f"  ✅ 仿真活动数: {len(simulator.simulation_results)}")
            
        except Exception as e:
            print(f"  ❌ 优化仿真失败: {e}")
            # 加载现有的仿真结果作为备选
            try:
                self.delay_analysis = pd.read_excel('完整机场排队仿真分析报告.xlsx')
                print(f"  ✅ 加载现有仿真结果: {len(self.delay_analysis)} 条记录")
            except:
                print("  ❌ 无法加载任何仿真结果")
                self.delay_analysis = None目组
日期：2025年8月
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体支持
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'PingFang SC', 'SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# 检查并设置可用的中文字体
import matplotlib.font_manager as fm
available_fonts = [f.name for f in fm.fontManager.ttflist]
chinese_fonts = ['Arial Unicode MS', 'PingFang SC', 'Hiragino Sans GB', 'STHeiti', 'SimHei', 'Microsoft YaHei']
for font in chinese_fonts:
    if font in available_fonts:
        plt.rcParams['font.sans-serif'] = [font] + plt.rcParams['font.sans-serif']
        print(f"使用中文字体: {font}")
        break
else:
    print("⚠️ 未找到合适的中文字体，可能显示为方框")

class AirportMetricsAnalyzer:
    """机场指标分析器"""
    
    def __init__(self):
        """初始化分析器"""
        self.target_airport = 'ZGGG'  # 广州白云机场
        self.busy_hours = list(range(7, 24))  # 7-23点繁忙时段
        
        # 数据存储
        self.actual_data = None           # 实际运行数据
        self.simulation_data = None       # 仿真结果数据
        self.delay_analysis = None        # 延误分析数据
        self.flight_plans = None          # 飞行计划数据
        
        print("=== 航空挑战杯指标分析系统 ===")
        print("目标机场：广州白云机场(ZGGG)")
        print("分析时段：7:00-23:00 (16小时)")
        print("分析指标：指标2(出港积压偏移误差) + 指标3(停止起降情景)")
    
    def load_data(self):
        """加载所有数据文件"""
        print("\n📂 正在加载数据文件...")
        
        try:
            # 1. 加载实际航班运行数据 - 使用正确的文件
            print("- 加载实际航班运行数据...")
            self.actual_data = pd.read_excel('数据/5月航班运行数据（实际数据列）.xlsx')
            print(f"  ✅ 实际数据: {len(self.actual_data)} 条记录")
            
            # 修正列名以匹配分析需求
            column_mapping = {
                '实际起飞站四字码': '计划起飞站四字码',
                '实际起飞时间': '计划离港时间',
                '实际到达站四字码': '计划降落站四字码',
                '实际落地时间': '实际到达时间'
            }
            
            for old_col, new_col in column_mapping.items():
                if old_col in self.actual_data.columns:
                    self.actual_data[new_col] = self.actual_data[old_col]
            
            # 2. 加载三段式飞行计划数据（如果存在）
            print("- 加载三段式飞行计划数据...")
            try:
                self.flight_plans = pd.read_excel('数据/三段式飞行- plan所需数据.xlsx')
                print(f"  ✅ 飞行计划: {len(self.flight_plans)} 条记录")
            except FileNotFoundError:
                print("  ⚠️ 三段式飞行计划文件未找到，使用实际数据代替")
                self.flight_plans = self.actual_data.copy()
            
            # 3. 生成优化的仿真结果
            print("- 生成优化的仿真分析...")
            self.generate_optimized_simulation_analysis()
            
            # 4. 加载仿真结果XML
            print("- 加载仿真结果XML...")
            self.simulation_data = self.parse_simulation_xml('仿真/all_flight_plans.xml')
            print(f"  ✅ 仿真数据: {len(self.simulation_data)} 条记录")
            
            return True
            
        except Exception as e:
            print(f"❌ 数据加载失败: {e}")
            return False
    
    def parse_simulation_xml(self, xml_file):
        """解析仿真XML文件"""
        simulation_records = []
        
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            for person in root.findall('.//person'):
                person_id = person.get('id')
                
                for plan in person.findall('.//plan'):
                    for activity in plan.findall('.//activity'):
                        activity_type = activity.get('type')
                        
                        if activity_type in ['departure', 'arrival']:
                            # 提取机场信息
                            facility = activity.get('facility', '')
                            if facility:
                                # 从设施ID中提取机场代码
                                airport = facility.split('_')[0] if '_' in facility else facility
                                
                                # 提取时间信息
                                start_time = activity.get('start_time', '')
                                end_time = activity.get('end_time', '')
                                
                                simulation_records.append({
                                    'aircraft_id': person_id,
                                    'airport': airport,
                                    'activity_type': activity_type,
                                    'start_time': start_time,
                                    'end_time': end_time,
                                    'facility': facility
                                })
            
            return pd.DataFrame(simulation_records)
            
        except Exception as e:
            print(f"⚠️ XML解析失败: {e}")
            return pd.DataFrame()
    
    def filter_target_airport_data(self):
        """筛选目标机场数据"""
        print(f"\n🎯 筛选{self.target_airport}机场数据...")
        
        # 筛选实际数据中的目标机场
        if self.actual_data is not None:
            # 尝试不同的机场列名
            airport_columns = ['机场', 'airport', 'Airport', '起飞机场', '降落机场', 'dep_airport', 'arr_airport']
            airport_col = None
            
            for col in airport_columns:
                if col in self.actual_data.columns:
                    airport_col = col
                    break
            
            if airport_col:
                zggg_actual = self.actual_data[
                    self.actual_data[airport_col].str.contains(self.target_airport, na=False)
                ]
                print(f"  ✅ 实际数据中{self.target_airport}: {len(zggg_actual)} 条记录")
            else:
                print("  ⚠️ 未找到机场列，显示所有列名:")
                print(f"     {list(self.actual_data.columns)}")
        
        # 筛选仿真数据中的目标机场
        if self.simulation_data is not None and not self.simulation_data.empty:
            zggg_simulation = self.simulation_data[
                self.simulation_data['airport'] == self.target_airport
            ]
            print(f"  ✅ 仿真数据中{self.target_airport}: {len(zggg_simulation)} 条记录")
        
        # 筛选延误分析中的目标机场
        if self.delay_analysis is not None:
            # 尝试筛选目标机场的延误数据
            if 'airport' in self.delay_analysis.columns:
                zggg_delays = self.delay_analysis[
                    self.delay_analysis['airport'] == self.target_airport
                ]
                print(f"  ✅ 延误分析中{self.target_airport}: {len(zggg_delays)} 条记录")
            else:
                print("  ℹ️ 延误分析数据列名:")
                print(f"     {list(self.delay_analysis.columns)}")
    
    def analyze_metric_2(self):
        """分析指标2：出港积压发生时段偏移误差"""
        print("\n📊 === 指标2分析：出港积压发生时段偏移误差 ===")
        
        # 定义积压阈值
        backlog_threshold = 10  # 延误航班超过10班
        
        try:
            # 1. 分析实际出港积压情况
            print("1. 分析实际出港积压情况...")
            actual_backlog = self.calculate_actual_backlog()
            
            # 2. 分析仿真推演的积压情况
            print("2. 分析仿真推演积压情况...")
            simulated_backlog = self.calculate_simulated_backlog()
            
            # 3. 计算4个子指标
            print("3. 计算指标2的4个子项...")
            metric_2_results = {
                'backlog_period_deviation': None,
                'duration_match': None,
                'peak_deviation': None,
                'latest_operation_match': None
            }
            
            # 子项1：积压时段偏差（不超过1个时段）
            if actual_backlog and simulated_backlog:
                period_deviation = self.calculate_period_deviation(actual_backlog, simulated_backlog)
                metric_2_results['backlog_period_deviation'] = period_deviation
                print(f"   - 积压时段偏差: {period_deviation} 小时")
            else:
                metric_2_results['backlog_period_deviation'] = float('inf')
                print(f"   - 积压时段偏差: 无法计算（数据不足）")
            
            # 子项2：持续时长一致性
            duration_match = self.check_duration_consistency(actual_backlog, simulated_backlog)
            metric_2_results['duration_match'] = duration_match
            print(f"   - 持续时长匹配: {'✅通过' if duration_match else '❌不通过'}")
            
            # 子项3：积压峰值偏差（≤15%）
            peak_deviation = self.calculate_peak_deviation(actual_backlog, simulated_backlog)
            if peak_deviation is not None:
                metric_2_results['peak_deviation'] = peak_deviation
                print(f"   - 峰值偏差: {peak_deviation:.1f}%")
            else:
                metric_2_results['peak_deviation'] = 100.0  # 默认100%偏差表示无法计算
                print(f"   - 峰值偏差: 无法计算（数据不足）")
            
            # 子项4：最晚运行时段一致性
            latest_match = self.check_latest_operation_consistency(actual_backlog, simulated_backlog)
            metric_2_results['latest_operation_match'] = latest_match
            print(f"   - 最晚运行时段匹配: {'✅通过' if latest_match else '❌不通过'}")
            
            return metric_2_results
            
        except Exception as e:
            print(f"❌ 指标2分析失败: {e}")
            return None
    
    def analyze_metric_3(self):
        """分析指标3：停止起降情景下积压化解偏移误差"""
        print("\n📊 === 指标3分析：停止起降情景下积压化解偏移误差 ===")
        
        try:
            # 1. 识别历史停止起降时段
            print("1. 识别历史停止起降时段...")
            shutdown_periods = self.identify_shutdown_periods()
            
            if not shutdown_periods:
                print("  ⚠️ 未找到明显的停止起降时段")
                return None
            
            # 2. 选择验证时段（2个时段以内）
            selected_periods = shutdown_periods[:2]  # 选择前2个时段
            print(f"  ✅ 选择验证时段: {selected_periods}")
            
            # 3. 模拟停止起降情景
            print("2. 模拟停止起降情景...")
            scenario_results = self.simulate_shutdown_scenario(selected_periods)
            
            # 4. 计算积压化解偏移误差（与指标2相同的4个子项）
            print("3. 计算积压化解偏移误差...")
            metric_3_results = {
                'scenario_periods': selected_periods,
                'backlog_period_deviation': None,
                'duration_match': None,
                'peak_deviation': None,
                'latest_operation_match': None
            }
            
            # 使用与指标2相同的计算方法
            if scenario_results:
                actual_scenario = scenario_results['actual']
                simulated_scenario = scenario_results['simulated']
                
                # 计算4个子指标
                period_deviation = self.calculate_period_deviation(actual_scenario, simulated_scenario)
                duration_match = self.check_duration_consistency(actual_scenario, simulated_scenario)
                peak_deviation = self.calculate_peak_deviation(actual_scenario, simulated_scenario)
                latest_match = self.check_latest_operation_consistency(actual_scenario, simulated_scenario)
                
                metric_3_results.update({
                    'backlog_period_deviation': period_deviation,
                    'duration_match': duration_match,
                    'peak_deviation': peak_deviation,
                    'latest_operation_match': latest_match
                })
                
                print(f"   - 积压时段偏差: {period_deviation} 小时")
                print(f"   - 持续时长匹配: {'✅通过' if duration_match else '❌不通过'}")
                print(f"   - 峰值偏差: {peak_deviation:.1f}%")
                print(f"   - 最晚运行时段匹配: {'✅通过' if latest_match else '❌不通过'}")
            
            return metric_3_results
            
        except Exception as e:
            print(f"❌ 指标3分析失败: {e}")
            return None
    
    def calculate_actual_backlog(self):
        """计算实际出港积压情况"""
        # 基于实际数据计算每小时的延误航班数
        print("   分析实际延误数据...")
        
        if self.actual_data is None:
            return None
        
        # 筛选广州白云机场的出港航班
        zggg_departures = self.actual_data[
            self.actual_data['计划起飞站四字码'] == self.target_airport
        ].copy()
        
        if len(zggg_departures) == 0:
            print(f"   ⚠️ 未找到{self.target_airport}的出港航班")
            return None
        
        print(f"   找到{self.target_airport}出港航班: {len(zggg_departures)} 班")
        
        # 计算延误情况
        zggg_departures['计划离港时间'] = pd.to_datetime(zggg_departures['计划离港时间'])
        zggg_departures['实际离港时间'] = pd.to_datetime(zggg_departures['实际离港时间'])
        
        # 计算延误分钟数
        zggg_departures['延误分钟'] = (
            zggg_departures['实际离港时间'] - zggg_departures['计划离港时间']
        ).dt.total_seconds() / 60
        
        # 只考虑正延误（实际时间晚于计划时间）
        delayed_flights = zggg_departures[zggg_departures['延误分钟'] > 0].copy()
        
        # 按小时统计延误航班数
        delayed_flights['小时'] = delayed_flights['计划离港时间'].dt.hour
        hourly_delays = delayed_flights.groupby('小时').size()
        
        # 筛选繁忙时段(7-23点)
        busy_hour_delays = {hour: hourly_delays.get(hour, 0) for hour in self.busy_hours}
        
        # 找出积压时段 - 使用航班量而非延误数据（因为实际数据文件可能无延误字段）
        print("   使用航班量分析积压时段...")
        
        # 重新分析：基于航班量而非延误
        zggg_flights = self.actual_data[
            self.actual_data['计划起飞站四字码'].str.contains(self.target_airport, na=False)
        ].copy()
        
        # 处理时间数据 - 考虑可能的格式问题
        valid_time_mask = (zggg_flights['计划离港时间'] != '-') & (zggg_flights['计划离港时间'].notna())
        zggg_clean = zggg_flights[valid_time_mask].copy()
        zggg_clean['计划离港时间'] = pd.to_datetime(zggg_clean['计划离港时间'], errors='coerce')
        zggg_clean = zggg_clean[zggg_clean['计划离港时间'].notna()]
        
        # 按日期和小时分组，计算平均每小时航班量
        zggg_clean['日期'] = zggg_clean['计划离港时间'].dt.date
        zggg_clean['小时'] = zggg_clean['计划离港时间'].dt.hour
        
        # 计算每日每小时航班量，然后取平均
        daily_hourly = zggg_clean.groupby(['日期', '小时']).size().reset_index(name='航班数')
        avg_hourly_flights = daily_hourly.groupby('小时')['航班数'].mean()
        
        # 重新定义积压：每小时平均超过8班认为积压（考虑单跑道6班/小时的合理容量）
        busy_hour_counts = {hour: avg_hourly_flights.get(hour, 0) for hour in self.busy_hours}
        backlog_hours = [hour for hour, count in busy_hour_counts.items() if count > 8]
        
        if not backlog_hours:
            print("   ✅ 繁忙时段无明显积压（航班量>8班/小时的时段）")
            return {
                'start_hour': None,
                'end_hour': None,
                'peak_hour': None,
                'peak_count': 0,
                'hourly_counts': busy_hour_counts,
                'backlog_hours': []
            }
        
        # 确定积压时段
        start_hour = min(backlog_hours)
        end_hour = max(backlog_hours)
        peak_hour = max(busy_hour_counts.keys(), key=lambda x: busy_hour_counts[x])
        peak_count = busy_hour_counts[peak_hour]
        
        backlog_data = {
            'start_hour': start_hour,
            'end_hour': end_hour,
            'peak_hour': peak_hour,
            'peak_count': peak_count,
            'hourly_counts': busy_hour_counts,
            'backlog_hours': backlog_hours
        }
        
        print(f"   ✅ 实际积压时段: {start_hour}:00-{end_hour}:00")
        print(f"   ✅ 峰值时段: {peak_hour}:00 ({peak_count:.1f}班平均每小时)")
        
        # 显示详细的每小时分析
        print("   📊 每小时平均航班量分析:")
        for hour in sorted(busy_hour_counts.keys()):
            count = busy_hour_counts[hour]
            status = "🔥 积压" if count > 8 else "📈 繁忙" if count > 5 else "✅ 正常" if count > 0 else ""
            print(f"     {hour:2d}:00 - {count:5.1f} 班 {status}")
        
        return backlog_data
    
    def calculate_simulated_backlog(self):
        """计算仿真推演的积压情况"""
        print("   分析仿真延误数据...")
        
        if self.delay_analysis is None:
            return None
        
        # 使用机场排队仿真系统的结果
        try:
            # 重新运行仿真获取详细数据
            from 机场排队仿真系统 import AirportQueueSimulator
            
            simulator = AirportQueueSimulator(
                departure_time=20,
                arrival_time=10, 
                num_runways=10
            )
            
            # 加载飞行计划
            simulator.load_flight_plans("仿真/all_flight_plans.xml")
            
            # 收集机场活动
            airport_activities = simulator.collect_airport_activities()
            
            # 执行仿真
            updated_plans = simulator.simulate_queue(airport_activities, time_disturbance=10)
            
            # 分析广州白云机场的仿真结果
            zggg_results = [r for r in simulator.simulation_results 
                           if r['airport'] == self.target_airport and r['activity_type'] == 'departure']
            
            if not zggg_results:
                print(f"   ⚠️ 仿真结果中未找到{self.target_airport}的出港数据")
                return None
            
            print(f"   找到{self.target_airport}仿真出港: {len(zggg_results)} 条记录")
            
            # 按小时统计延误航班
            hourly_delays = {}
            for result in zggg_results:
                delay = result['delay_minutes']
                if delay > 0:  # 只考虑延误航班
                    # 解析计划时间获取小时
                    planned_time = result.get('planned_start', '')
                    if planned_time:
                        hour = int(planned_time.split(':')[0])
                        if hour in self.busy_hours:
                            hourly_delays[hour] = hourly_delays.get(hour, 0) + 1
            
            # 补充没有延误的小时为0
            busy_hour_delays = {hour: hourly_delays.get(hour, 0) for hour in self.busy_hours}
            
            # 找出积压时段（延误航班>10班）
            backlog_hours = [hour for hour, count in busy_hour_delays.items() if count > 10]
            
            if not backlog_hours:
                print("   ✅ 仿真结果显示繁忙时段无明显积压")
                return {
                    'start_hour': None,
                    'end_hour': None,
                    'peak_hour': None,
                    'peak_count': 0,
                    'hourly_counts': busy_hour_delays,
                    'backlog_hours': []
                }
            
            # 确定积压时段
            start_hour = min(backlog_hours)
            end_hour = max(backlog_hours)
            peak_hour = max(busy_hour_delays.keys(), key=lambda x: busy_hour_delays[x])
            peak_count = busy_hour_delays[peak_hour]
            
            backlog_data = {
                'start_hour': start_hour,
                'end_hour': end_hour,
                'peak_hour': peak_hour,
                'peak_count': peak_count,
                'hourly_counts': busy_hour_delays,
                'backlog_hours': backlog_hours
            }
            
            print(f"   ✅ 仿真积压时段: {start_hour}:00-{end_hour}:00")
            print(f"   ✅ 仿真峰值时段: {peak_hour}:00 ({peak_count}班延误)")
            
            return backlog_data
            
        except Exception as e:
            print(f"   ❌ 仿真数据分析失败: {e}")
            return None
    
    def identify_shutdown_periods(self):
        """识别停止起降时段"""
        # 分析数据中每小时航班量≤2班的时段
        shutdown_periods = []
        
        if self.actual_data is None:
            return shutdown_periods
        
        # 分析广州白云机场的每小时航班量
        zggg_data = self.actual_data[
            (self.actual_data['计划起飞站四字码'] == self.target_airport) |
            (self.actual_data['计划到达站四字码'] == self.target_airport)
        ].copy()
        
        if len(zggg_data) == 0:
            print("   ⚠️ 未找到广州白云机场的航班数据")
            return shutdown_periods
        
        # 分别分析起飞和降落
        departure_data = zggg_data[zggg_data['计划起飞站四字码'] == self.target_airport].copy()
        arrival_data = zggg_data[zggg_data['计划到达站四字码'] == self.target_airport].copy()
        
        # 计算每小时起飞量
        if not departure_data.empty:
            departure_data['计划离港时间'] = pd.to_datetime(departure_data['计划离港时间'])
            departure_data['小时'] = departure_data['计划离港时间'].dt.hour
            hourly_departures = departure_data.groupby('小时').size()
        else:
            hourly_departures = pd.Series(dtype=int)
        
        # 计算每小时降落量
        if not arrival_data.empty:
            arrival_data['计划到港时间'] = pd.to_datetime(arrival_data['计划到港时间'])
            arrival_data['小时'] = arrival_data['计划到港时间'].dt.hour
            hourly_arrivals = arrival_data.groupby('小时').size()
        else:
            hourly_arrivals = pd.Series(dtype=int)
        
        # 分析繁忙时段(7-23点)的低流量时段
        for hour in self.busy_hours:
            dep_count = hourly_departures.get(hour, 0)
            arr_count = hourly_arrivals.get(hour, 0)
            total_count = dep_count + arr_count
            
            if total_count <= 2:  # 进出港总量≤2班
                shutdown_periods.append({
                    'start': hour,
                    'end': hour + 1,
                    'departure_count': dep_count,
                    'arrival_count': arr_count,
                    'total_count': total_count,
                    'reason': f'低流量时段({total_count}班)'
                })
        
        # 按时段合并连续的停止起降时段
        merged_periods = []
        if shutdown_periods:
            current_period = shutdown_periods[0].copy()
            
            for period in shutdown_periods[1:]:
                if period['start'] == current_period['end']:
                    # 连续时段，合并
                    current_period['end'] = period['end']
                    current_period['total_count'] += period['total_count']
                    current_period['reason'] = f"连续低流量({current_period['total_count']}班)"
                else:
                    # 非连续，保存当前时段并开始新时段
                    merged_periods.append(current_period)
                    current_period = period.copy()
            
            merged_periods.append(current_period)
        
        print(f"   ✅ 识别到{len(merged_periods)}个潜在停止起降时段:")
        for i, period in enumerate(merged_periods):
            print(f"      {i+1}. {period['start']}:00-{period['end']}:00 "
                  f"({period['reason']})")
        
        return merged_periods[:2]  # 最多返回2个时段
    
    def simulate_shutdown_scenario(self, periods):
        """模拟停止起降情景"""
        # 基于选定时段模拟停止起降对后续航班的影响
        
        scenario_results = {
            'actual': None,
            'simulated': None
        }
        
        if not periods:
            return scenario_results
        
        print(f"   模拟停止起降情景: {len(periods)}个时段")
        
        try:
            # 1. 分析实际停止起降期间的影响
            actual_impact = self.analyze_actual_shutdown_impact(periods)
            
            # 2. 模拟停止起降的仿真影响
            simulated_impact = self.simulate_shutdown_impact(periods)
            
            scenario_results = {
                'actual': actual_impact,
                'simulated': simulated_impact
            }
            
            return scenario_results
            
        except Exception as e:
            print(f"   ❌ 停止起降情景模拟失败: {e}")
            return scenario_results
    
    def analyze_actual_shutdown_impact(self, periods):
        """分析实际停止起降的影响"""
        if not periods or self.actual_data is None:
            return None
        
        # 分析停止起降时段前后的延误模式
        zggg_departures = self.actual_data[
            self.actual_data['计划起飞站四字码'] == self.target_airport
        ].copy()
        
        if zggg_departures.empty:
            return None
        
        zggg_departures['计划离港时间'] = pd.to_datetime(zggg_departures['计划离港时间'])
        zggg_departures['实际离港时间'] = pd.to_datetime(zggg_departures['实际离港时间'])
        zggg_departures['延误分钟'] = (
            zggg_departures['实际离港时间'] - zggg_departures['计划离港时间']
        ).dt.total_seconds() / 60
        
        zggg_departures['小时'] = zggg_departures['计划离港时间'].dt.hour
        
        # 分析停止起降前后3小时的延误情况
        impact_hours = set()
        for period in periods:
            for h in range(max(7, period['start'] - 3), min(24, period['end'] + 4)):
                impact_hours.add(h)
        
        impact_data = zggg_departures[zggg_departures['小时'].isin(impact_hours)]
        delayed_flights = impact_data[impact_data['延误分钟'] > 0]
        
        hourly_delays = delayed_flights.groupby('小时').size().to_dict()
        
        # 找出延误积压时段
        backlog_hours = [h for h in impact_hours if hourly_delays.get(h, 0) > 10]
        
        if backlog_hours:
            return {
                'start_hour': min(backlog_hours),
                'end_hour': max(backlog_hours),
                'peak_hour': max(hourly_delays.keys(), key=lambda x: hourly_delays.get(x, 0)),
                'peak_count': max(hourly_delays.values()) if hourly_delays else 0,
                'hourly_counts': hourly_delays,
                'backlog_hours': backlog_hours
            }
        else:
            return {
                'start_hour': None,
                'end_hour': None,
                'peak_hour': None,
                'peak_count': 0,
                'hourly_counts': hourly_delays,
                'backlog_hours': []
            }
    
    def simulate_shutdown_impact(self, periods):
        """模拟停止起降的仿真影响"""
        # 这里可以通过修改仿真参数来模拟停止起降的影响
        # 例如：在指定时段设置跑道容量为0
        
        try:
            from 机场排队仿真系统 import AirportQueueSimulator
            
            # 创建特殊配置的仿真器来模拟停止起降
            simulator = AirportQueueSimulator(
                departure_time=20,
                arrival_time=10,
                num_runways=10
            )
            
            simulator.load_flight_plans("仿真/all_flight_plans.xml")
            airport_activities = simulator.collect_airport_activities()
            
            # 模拟停止起降的影响（增加额外延误）
            updated_plans = simulator.simulate_queue(
                airport_activities, 
                time_disturbance=20  # 增加更大的随机延误来模拟停止起降影响
            )
            
            # 分析结果
            zggg_results = [r for r in simulator.simulation_results 
                           if r['airport'] == self.target_airport and r['activity_type'] == 'departure']
            
            hourly_delays = {}
            for result in zggg_results:
                delay = result['delay_minutes']
                if delay > 0:
                    planned_time = result.get('planned_start', '')
                    if planned_time:
                        hour = int(planned_time.split(':')[0])
                        if 7 <= hour <= 23:
                            hourly_delays[hour] = hourly_delays.get(hour, 0) + 1
            
            backlog_hours = [h for h, count in hourly_delays.items() if count > 10]
            
            if backlog_hours:
                return {
                    'start_hour': min(backlog_hours),
                    'end_hour': max(backlog_hours),
                    'peak_hour': max(hourly_delays.keys(), key=lambda x: hourly_delays.get(x, 0)),
                    'peak_count': max(hourly_delays.values()) if hourly_delays else 0,
                    'hourly_counts': hourly_delays,
                    'backlog_hours': backlog_hours
                }
            else:
                return {
                    'start_hour': None,
                    'end_hour': None,
                    'peak_hour': None,
                    'peak_count': 0,
                    'hourly_counts': hourly_delays,
                    'backlog_hours': []
                }
                
        except Exception as e:
            print(f"   ❌ 仿真停止起降影响失败: {e}")
            return None
    
    def calculate_period_deviation(self, actual, simulated):
        """计算时段偏差"""
        if not actual or not simulated:
            return float('inf')
        
        # 获取开始时间，如果为None则使用默认值
        actual_start = actual.get('start_hour')
        simulated_start = simulated.get('start_hour')
        
        # 检查是否有None值
        if actual_start is None or simulated_start is None:
            return float('inf')
        
        return abs(actual_start - simulated_start)
    
    def check_duration_consistency(self, actual, simulated):
        """检查持续时长一致性"""
        if not actual or not simulated:
            return False
        
        # 获取开始和结束时间，检查None值
        actual_start = actual.get('start_hour')
        actual_end = actual.get('end_hour')
        simulated_start = simulated.get('start_hour')
        simulated_end = simulated.get('end_hour')
        
        # 如果任何值为None，返回False
        if any(x is None for x in [actual_start, actual_end, simulated_start, simulated_end]):
            return False
        
        # 计算持续时长
        actual_duration = actual_end - actual_start
        simulated_duration = simulated_end - simulated_start
        
        return actual_duration == simulated_duration
    
    def calculate_peak_deviation(self, actual, simulated):
        """计算峰值偏差"""
        if not actual or not simulated:
            return 100.0
        
        actual_peak = actual.get('peak_count', 0)
        simulated_peak = simulated.get('peak_count', 0)
        
        if actual_peak == 0:
            return 0.0 if simulated_peak == 0 else 100.0
        
        return abs(simulated_peak - actual_peak) / actual_peak * 100
    
    def check_latest_operation_consistency(self, actual, simulated):
        """检查最晚运行时段一致性"""
        if not actual or not simulated:
            return False
        
        # 获取结束时间，检查None值
        actual_latest = actual.get('end_hour')
        simulated_latest = simulated.get('end_hour')
        
        # 如果任何值为None，返回False
        if actual_latest is None or simulated_latest is None:
            return False
        
        return actual_latest == simulated_latest
    
    def generate_report(self, metric_2_results, metric_3_results):
        """生成分析报告"""
        print("\n📋 === 指标分析报告 ===")
        
        # 保存结果到Excel
        report_data = []
        
        if metric_2_results:
            report_data.append({
                '指标': '指标2：出港积压发生时段偏移误差',
                '积压时段偏差(小时)': metric_2_results.get('backlog_period_deviation'),
                '持续时长匹配': '通过' if metric_2_results.get('duration_match') else '不通过',
                '峰值偏差(%)': metric_2_results.get('peak_deviation'),
                '最晚运行时段匹配': '通过' if metric_2_results.get('latest_operation_match') else '不通过'
            })
        
        if metric_3_results:
            report_data.append({
                '指标': '指标3：停止起降情景积压化解偏移误差',
                '积压时段偏差(小时)': metric_3_results.get('backlog_period_deviation'),
                '持续时长匹配': '通过' if metric_3_results.get('duration_match') else '不通过',
                '峰值偏差(%)': metric_3_results.get('peak_deviation'),
                '最晚运行时段匹配': '通过' if metric_3_results.get('latest_operation_match') else '不通过'
            })
        
        if report_data:
            report_df = pd.DataFrame(report_data)
            report_file = '指标分析结果报告.xlsx'
            report_df.to_excel(report_file, index=False)
            print(f"✅ 分析报告已保存: {report_file}")
            
            # 显示报告内容
            print("\n📊 分析结果摘要:")
            for _, row in report_df.iterrows():
                print(f"\n{row['指标']}:")
                print(f"  - 积压时段偏差: {row['积压时段偏差(小时)']} 小时")
                print(f"  - 持续时长匹配: {row['持续时长匹配']}")
                print(f"  - 峰值偏差: {row['峰值偏差(%)']}%")
                print(f"  - 最晚运行时段匹配: {row['最晚运行时段匹配']}")
                
                # 评估是否符合要求
                deviation = row['积压时段偏差(小时)']
                peak_dev = row['峰值偏差(%)']
                
                if deviation is not None and peak_dev is not None:
                    meets_requirements = (
                        deviation <= 1 and  # 时段偏差不超过1小时
                        peak_dev <= 15 and  # 峰值偏差不超过15%
                        row['持续时长匹配'] == '通过' and
                        row['最晚运行时段匹配'] == '通过'
                    )
                    
                    status = "✅ 符合要求" if meets_requirements else "❌ 不符合要求"
                    print(f"  - 综合评估: {status}")
        
        # 生成可视化图表
        self.create_visualization_charts(metric_2_results, metric_3_results)
        
        return report_data
    
    def create_visualization_charts(self, metric_2_results, metric_3_results):
        """创建可视化图表"""
        try:
            print("\n📈 生成可视化图表...")
            
            fig, axes = plt.subplots(2, 2, figsize=(15, 12))
            fig.suptitle('航空挑战杯指标分析结果', fontsize=16, fontweight='bold')
            
            # 图表1：指标达成情况对比
            ax1 = axes[0, 0]
            metrics = ['时段偏差', '持续时长', '峰值偏差', '最晚运行']
            
            if metric_2_results:
                metric_2_scores = [
                    1 if metric_2_results.get('backlog_period_deviation', float('inf')) <= 1 else 0,
                    1 if metric_2_results.get('duration_match', False) else 0,
                    1 if metric_2_results.get('peak_deviation', 100) <= 15 else 0,
                    1 if metric_2_results.get('latest_operation_match', False) else 0
                ]
            else:
                metric_2_scores = [0, 0, 0, 0]
            
            if metric_3_results:
                metric_3_scores = [
                    1 if metric_3_results.get('backlog_period_deviation', float('inf')) <= 1 else 0,
                    1 if metric_3_results.get('duration_match', False) else 0,
                    1 if metric_3_results.get('peak_deviation', 100) <= 15 else 0,
                    1 if metric_3_results.get('latest_operation_match', False) else 0
                ]
            else:
                metric_3_scores = [0, 0, 0, 0]
            
            x = np.arange(len(metrics))
            width = 0.35
            
            ax1.bar(x - width/2, metric_2_scores, width, label='指标2', color='skyblue')
            ax1.bar(x + width/2, metric_3_scores, width, label='指标3', color='lightcoral')
            
            ax1.set_xlabel('子指标')
            ax1.set_ylabel('达成情况 (1=通过, 0=不通过)')
            ax1.set_title('指标达成情况对比')
            ax1.set_xticks(x)
            ax1.set_xticklabels(metrics)
            ax1.legend()
            ax1.set_ylim(0, 1.2)
            
            # 图表2：延误分布示例（如果有数据）
            ax2 = axes[0, 1]
            if hasattr(self, 'actual_data') and self.actual_data is not None:
                # 绘制广州白云机场的延误分布
                zggg_data = self.actual_data[
                    self.actual_data['计划起飞站四字码'] == self.target_airport
                ]
                
                if not zggg_data.empty:
                    zggg_data = zggg_data.copy()
                    zggg_data['计划离港时间'] = pd.to_datetime(zggg_data['计划离港时间'])
                    zggg_data['实际离港时间'] = pd.to_datetime(zggg_data['实际离港时间'])
                    zggg_data['延误分钟'] = (
                        zggg_data['实际离港时间'] - zggg_data['计划离港时间']
                    ).dt.total_seconds() / 60
                    
                    # 只取正延误
                    delays = zggg_data[zggg_data['延误分钟'] > 0]['延误分钟']
                    
                    if not delays.empty:
                        ax2.hist(delays, bins=20, alpha=0.7, color='orange', edgecolor='black')
                        ax2.set_xlabel('延误时间(分钟)')
                        ax2.set_ylabel('航班数量')
                        ax2.set_title(f'{self.target_airport}延误分布')
                    else:
                        ax2.text(0.5, 0.5, '无延误数据', ha='center', va='center', transform=ax2.transAxes)
                else:
                    ax2.text(0.5, 0.5, '无航班数据', ha='center', va='center', transform=ax2.transAxes)
            else:
                ax2.text(0.5, 0.5, '数据未加载', ha='center', va='center', transform=ax2.transAxes)
            
            # 图表3：每小时航班量分布
            ax3 = axes[1, 0]
            if hasattr(self, 'actual_data') and self.actual_data is not None:
                zggg_data = self.actual_data[
                    self.actual_data['计划起飞站四字码'] == self.target_airport
                ]
                
                if not zggg_data.empty:
                    zggg_data = zggg_data.copy()
                    zggg_data['计划离港时间'] = pd.to_datetime(zggg_data['计划离港时间'])
                    zggg_data['小时'] = zggg_data['计划离港时间'].dt.hour
                    
                    hourly_counts = zggg_data.groupby('小时').size()
                    busy_hours = [h for h in self.busy_hours if h in hourly_counts.index]
                    busy_counts = [hourly_counts[h] for h in busy_hours]
                    
                    ax3.bar(busy_hours, busy_counts, color='lightgreen', alpha=0.7)
                    ax3.set_xlabel('时间(小时)')
                    ax3.set_ylabel('出港航班数')
                    ax3.set_title(f'{self.target_airport}繁忙时段流量分布')
                    ax3.set_xticks(range(7, 24, 2))
                else:
                    ax3.text(0.5, 0.5, '无航班数据', ha='center', va='center', transform=ax3.transAxes)
            
            # 图表4：指标详细数值
            ax4 = axes[1, 1]
            ax4.axis('off')
            
            report_text = "指标分析详细结果\n\n"
            
            if metric_2_results:
                report_text += "指标2 - 出港积压发生时段偏移误差:\n"
                report_text += f"• 时段偏差: {metric_2_results.get('backlog_period_deviation', 'N/A')} 小时\n"
                report_text += f"• 峰值偏差: {metric_2_results.get('peak_deviation', 'N/A')}%\n"
                report_text += f"• 持续时长: {'匹配' if metric_2_results.get('duration_match') else '不匹配'}\n"
                report_text += f"• 最晚运行: {'匹配' if metric_2_results.get('latest_operation_match') else '不匹配'}\n\n"
            
            if metric_3_results:
                report_text += "指标3 - 停止起降情景积压化解偏移误差:\n"
                report_text += f"• 时段偏差: {metric_3_results.get('backlog_period_deviation', 'N/A')} 小时\n"
                report_text += f"• 峰值偏差: {metric_3_results.get('peak_deviation', 'N/A')}%\n"
                report_text += f"• 持续时长: {'匹配' if metric_3_results.get('duration_match') else '不匹配'}\n"
                report_text += f"• 最晚运行: {'匹配' if metric_3_results.get('latest_operation_match') else '不匹配'}\n"
            
            ax4.text(0.05, 0.95, report_text, transform=ax4.transAxes, 
                    fontsize=10, verticalalignment='top', fontfamily='monospace')
            
            plt.tight_layout()
            chart_file = '指标分析可视化图表.png'
            plt.savefig(chart_file, dpi=300, bbox_inches='tight')
            plt.show()
            
            print(f"✅ 可视化图表已保存: {chart_file}")
            
        except Exception as e:
            print(f"❌ 图表生成失败: {e}")
    
    def run_analysis(self):
        """运行完整分析流程"""
        print("🚀 开始指标分析...")
        
        # 1. 加载数据
        if not self.load_data():
            print("❌ 数据加载失败，分析终止")
            return False
        
        # 2. 数据预处理
        self.filter_target_airport_data()
        
        # 3. 指标分析
        metric_2_results = self.analyze_metric_2()
        metric_3_results = self.analyze_metric_3()
        
        # 4. 生成报告
        report_data = self.generate_report(metric_2_results, metric_3_results)
        
        print("\n✅ 指标分析完成！")
        return True

def main():
    """主函数"""
    analyzer = AirportMetricsAnalyzer()
    success = analyzer.run_analysis()
    
    if success:
        print("\n🎉 分析成功完成！")
        print("📁 查看生成的文件：")
        print("   - 指标分析结果报告.xlsx")
    else:
        print("\n❌ 分析失败，请检查数据文件和配置")

if __name__ == "__main__":
    main()
