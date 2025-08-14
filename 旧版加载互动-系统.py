#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG Airport Takeoff Simulation System (Enhanced Version)

This script combines a flight departure simulation with an interactive configuration
module. It allows users to set various parameters, including weather suspensions
and tower efficiency, to model their impact on airport operations.
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

# Set Chinese fonts for matplotlib
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False


class ZGGGDepartureSimulator:
    """
    An enhanced simulator for ZGGG airport departures, featuring interactive
    configuration for weather events and tower efficiency.
    """

    def __init__(self, delay_threshold=15, backlog_threshold=10, taxi_out_time=15,
                 flight_data_path=None, suspension_periods=None, efficiency_periods=None):
        """
        Initializes the simulator with user-defined parameters.

        Args:
            delay_threshold (int): The time in minutes after which a flight is considered delayed.
            backlog_threshold (int): The number of delayed flights per hour to define a backlog period.
            taxi_out_time (int): The standard time in minutes for taxiing out.
            flight_data_path (str): Path to the flight data .xlsx file.
            suspension_periods (list): A list of dictionaries for flight suspension periods.
            efficiency_periods (list): A list of dictionaries for tower low-efficiency periods.
        """
        self.delay_threshold = delay_threshold
        self.backlog_threshold = backlog_threshold
        self.taxi_out_time = taxi_out_time
        self.flight_data_path = flight_data_path
        self.suspension_periods = suspension_periods if suspension_periods else []
        self.efficiency_periods = efficiency_periods if efficiency_periods else []
        self.data = None
        self.all_simulation_results = pd.DataFrame()

        print("\n" + "="*50)
        print("=== ZGGG Departure Simulator Initialized ===")
        print(f"Delay Threshold: {self.delay_threshold} minutes")
        print(f"Backlog Threshold: {self.backlog_threshold} flights/hour")
        print(f"Standard Taxi-out Time: {self.taxi_out_time} minutes")
        print(f"Flight Data Path: {self.flight_data_path}")
        print(f"Custom Suspension Periods: {len(self.suspension_periods)}")
        print(f"Custom Efficiency Periods: {len(self.efficiency_periods)}")
        print("="*50)


    def load_and_prepare_data(self):
        """Loads and preprocesses flight data from the specified Excel file."""
        print("\n=== Step 1: Loading and Preparing Data ===")
        
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
            df = pd.read_excel(self.flight_data_path)
            print(f"成功加载数据文件: {self.flight_data_path}. 总记录数: {len(df)}")
        except Exception as e:
            print(f"加载数据文件时出错: {str(e)}")
            return False

        zggg_departures = df[df['实际起飞站四字码'] == 'ZGGG'].copy()
        print(f"Found {len(zggg_departures)} departing flights from ZGGG.")

        time_fields = ['计划离港时间', '实际离港时间', '实际起飞时间']
        for field in time_fields:
            zggg_departures[field] = pd.to_datetime(zggg_departures[field], errors='coerce')

        key_fields = ['航班号', '机型', '唯一序列号', '计划离港时间', '实际离港时间']
        valid_flights = zggg_departures.dropna(subset=key_fields).copy()
        print(f"Flights with essential data: {len(valid_flights)}")
        
        has_takeoff_time = valid_flights['实际起飞时间'].notna()
        complete_flights = valid_flights[has_takeoff_time].copy()
        missing_takeoff_flights = valid_flights[~has_takeoff_time].copy()
        
        if not missing_takeoff_flights.empty:
            missing_takeoff_flights['实际起飞时间'] = (
                missing_takeoff_flights['实际离港时间'] + pd.Timedelta(minutes=20)
            )
            print(f"Estimated takeoff time for {len(missing_takeoff_flights)} flights.")
        
        all_flights = pd.concat([complete_flights, missing_takeoff_flights], ignore_index=True)
        
        # Filter out logical errors
        all_flights = all_flights[all_flights['实际离港时间'] <= all_flights['实际起飞时间']].copy()
        
        all_flights['起飞延误分钟'] = (
            all_flights['实际起飞时间'] - all_flights['计划离港时间']
        ).dt.total_seconds() / 60
        
        all_flights['跑道'] = np.random.choice(['02R/20L', '02L/20R'], len(all_flights))
        
        self.data = all_flights
        print(f"Data preparation complete. Total flights for simulation: {len(self.data)}")
        return True

    def classify_aircraft_types(self):
        """Classifies aircraft into categories and sets Runway Occupancy Time (ROT)."""
        print("\n=== Step 2: Classifying Aircraft Types and Setting Parameters ===")
        if self.data is None:
            print("ERROR: Data not loaded.")
            return

        aircraft_categories = {
            'Heavy': {'types': ['773', '772', '77W', '77L', '744', '748', '380', '359', '35K'], 'rot_seconds': 105},
            'Medium': {'types': ['32G', '32N', '32A', '321', '320', '319', '32S', '73M', '738', '73G', '73H'], 'rot_seconds': 85},
            'Light': {'types': ['AT7', 'CR9', 'CRJ', 'E90', 'ERJ', 'E75'], 'rot_seconds': 70},
            'Cargo': {'types': ['76F', '77F', '74F', '32P', '737F'], 'rot_seconds': 115}
        }

        def get_aircraft_category(aircraft_type):
            for category, info in aircraft_categories.items():
                if aircraft_type in info['types']:
                    return category, info['rot_seconds']
            return 'Medium', 85 # Default

        self.data[['机型类别', 'ROT秒']] = self.data['机型'].apply(
            lambda x: pd.Series(get_aircraft_category(x))
        )
        
        self.wake_separation_matrix = {
            # (Lead, Follow) -> Seconds
            ('Heavy', 'Heavy'): 90, ('Heavy', 'Medium'): 120, ('Heavy', 'Light'): 180,
            ('Medium', 'Heavy'): 75, ('Medium', 'Medium'): 90, ('Medium', 'Light'): 120,
            ('Light', 'Heavy'): 75, ('Light', 'Medium'): 75, ('Light', 'Light'): 90,
        }
        
        print("Aircraft classification and wake separation rules are set.")
        category_stats = self.data['机型类别'].value_counts(normalize=True) * 100
        print("Aircraft category distribution:")
        for cat, perc in category_stats.items():
            print(f"  - {cat}: {perc:.1f}%")

    def run_simulation_for_date_range(self, start_date, end_date):
        """Runs the runway queue simulation for a given range of dates."""
        print("\n=== Step 3: Running Simulation ===")
        all_results = []
        date_range = pd.date_range(start_date, end_date)
        
        # 预处理跨天事件
        for sim_date in date_range:
            sim_date_obj = sim_date.date()
            print(f"\n--- Simulating for date: {sim_date_obj} ---")
            
            day_flights = self.data[self.data['计划离港时间'].dt.date == sim_date_obj].copy()
            if day_flights.empty:
                print("No flights scheduled for this date.")
                continue
                
            day_flights.sort_values('计划离港时间', inplace=True)
            print(f"Total flights for the day: {len(day_flights)}")

            # 检查当天的事件
            day_suspensions = []
            for p in self.suspension_periods:
                if p['date'] == sim_date_obj:
                    if p['start_time'].date() <= sim_date_obj <= p['end_time'].date():
                        day_suspensions.append(p)
        
            day_efficiency_mods = []
            for p in self.efficiency_periods:
                if p['date'] == sim_date_obj:
                    if p['start_time'].date() <= sim_date_obj <= p['end_time'].date():
                        day_efficiency_mods.append(p)

            if day_suspensions:
                print(f"Found {len(day_suspensions)} suspension period(s).")
            if day_efficiency_mods:
                print(f"Found {len(day_efficiency_mods)} tower efficiency reduction period(s).")

            sim_results_today = self._simulate_runway_queue(day_flights, day_suspensions, day_efficiency_mods)
            if sim_results_today:
                all_results.extend(sim_results_today)

        if not all_results:
            print("\nSimulation did not produce any results.")
            return False

        self.all_simulation_results = pd.DataFrame(all_results)
        print("\n=== Simulation for all dates complete! ===")
        return True

    def _simulate_runway_queue(self, day_flights, suspensions, efficiencies):
        """The core logic for simulating the runway queue for a single day."""
        runway_last_departure = {'02R/20L': None, '02L/20R': None}
        results = []

        # Get flights to be delayed due to priority rules
        priority_delayed_flights = set()
        for eff in efficiencies:
            if eff['type'] == '优先级延误':
                # 跨天效率处理
                start, end = eff['start_time'], eff['end_time']
                small_flights = day_flights[
                    (day_flights['计划离港时间'] >= start) & 
                    (day_flights['计划离港时间'] <= end) &
                    (day_flights['机型类别'].isin(['Light', 'Medium']))
                ]
                priority_delayed_flights.update(small_flights.index)

        for idx, flight in day_flights.iterrows():
            selected_runway = flight['跑道']
            planned_departure = flight['计划离港时间']
            base_takeoff_time = planned_departure + pd.Timedelta(minutes=self.taxi_out_time)
            
            # 1. Check for flight suspension
            is_suspended = False
            for period in suspensions:
                if period['start_time'] <= planned_departure <= period['end_time']:
                    base_takeoff_time = max(base_takeoff_time, period['end_time'])
                    is_suspended = True
                    break

            # 2. Check for tower efficiency reduction
            tower_efficiency = 1.0
            for period in efficiencies:
                if period['start_time'] <= planned_departure <= period['end_time']:
                    # Apply delay based on type
                    if (period['type'] == '随机延误' and np.random.rand() > 0.5) or \
                       (period['type'] == '按顺序延误') or \
                       (period['type'] == '优先级延误' and idx in priority_delayed_flights):
                        tower_efficiency = period['efficiency']
                    break
            
            # 3. Calculate runway availability
            last_flight_info = runway_last_departure[selected_runway]
            min_takeoff_time = base_takeoff_time

            if last_flight_info:
                last_takeoff_time = last_flight_info['sim_takeoff']
                last_wake_cat = last_flight_info['wake_category']
                
                # Apply tower efficiency to separation time
                wake_separation_secs = self.wake_separation_matrix.get((last_wake_cat, flight['机型类别']), 90)
                effective_separation_secs = wake_separation_secs / tower_efficiency
                
                runway_free_time = last_takeoff_time + pd.Timedelta(seconds=effective_separation_secs)
                min_takeoff_time = max(min_takeoff_time, runway_free_time)

            simulated_takeoff = min_takeoff_time
            delay_minutes = (simulated_takeoff - planned_departure).total_seconds() / 60

            results.append({
                '航班号': flight['航班号'],
                '机型类别': flight['机型类别'],
                '跑道': selected_runway,
                '计划起飞': planned_departure,
                '仿真起飞时间': simulated_takeoff,
                '仿真延误分钟': delay_minutes,
                '受天气影响': is_suspended,
                '实际延误分钟': flight['起飞延误分钟']
            })

            runway_last_departure[selected_runway] = {
                'sim_takeoff': simulated_takeoff,
                'wake_category': flight['机型类别']
            }
        
        return results

    def analyze_and_visualize_results(self):
        """Analyzes and visualizes the simulation results."""
        print("\n=== Step 4: Analyzing and Visualizing Results ===")
        if self.all_simulation_results.empty:
            print("No simulation results to analyze.")
            return

        results = self.all_simulation_results
        results['hour'] = results['计划起飞'].dt.hour
        
        avg_delay = results['仿真延误分钟'].mean()
        delay_rate = (results['仿真延误分钟'] > self.delay_threshold).sum() / len(results) * 100
        
        print(f"\n--- Overall Simulation Statistics ---")
        print(f"Average Simulated Delay: {avg_delay:.1f} minutes")
        print(f"Simulated Delay Rate (> {self.delay_threshold} min): {delay_rate:.1f}%")

        hourly_delays = results.groupby('hour')['仿真延误分钟'].mean()
        hourly_backlog = results[results['仿真延误分钟'] > self.delay_threshold].groupby('hour').size()
        
        fig, axes = plt.subplots(2, 2, figsize=(18, 14))
        fig.suptitle('ZGGG Takeoff Simulation Analysis', fontsize=16)

        # 1. Average delay per hour
        sns.barplot(x=hourly_delays.index, y=hourly_delays.values, ax=axes[0, 0], color='skyblue')
        axes[0, 0].set_title('Average Simulated Delay by Hour')
        axes[0, 0].set_xlabel('Hour of Day')
        axes[0, 0].set_ylabel('Average Delay (minutes)')
        axes[0, 0].grid(True, alpha=0.3)

        # 2. Backlog flights per hour
        sns.barplot(x=hourly_backlog.index, y=hourly_backlog.values, ax=axes[0, 1], color='salmon')
        axes[0, 1].axhline(y=self.backlog_threshold, color='red', linestyle='--', label=f'Backlog Threshold ({self.backlog_threshold})')
        axes[0, 1].set_title('Number of Backlogged Flights by Hour')
        axes[0, 1].set_xlabel('Hour of Day')
        axes[0, 1].set_ylabel('Number of Flights')
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3)
        
        # 3. Delay distribution
        sns.histplot(results['仿真延误分钟'], bins=50, kde=True, ax=axes[1, 0])
        axes[1, 0].set_title('Distribution of Simulated Delays')
        axes[1, 0].set_xlabel('Delay (minutes)')
        axes[1, 0].set_ylabel('Frequency')
        axes[1, 0].grid(True, alpha=0.3)

        # 4. Runway Usage
        runway_usage = results['跑道'].value_counts()
        axes[1, 1].pie(runway_usage, labels=runway_usage.index, autopct='%1.1f%%', startangle=90, colors=['#ff9999','#66b3ff'])
        axes[1, 1].set_title('Simulated Runway Usage')
        axes[1, 1].set_ylabel('')

        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        output_filename = 'ZGGG_Simulation_Analysis.png'
        plt.savefig(output_filename, dpi=300)
        print(f"\nAnalysis chart saved as '{output_filename}'")
        # plt.show() # Disabled for non-interactive environments


def get_user_input():
    """Collects simulation parameters from the user via a CLI."""
    print("="*60)
    print("      Welcome to the ZGGG Interactive Departure Simulator")
    print("="*60)
    
    # --- Basic Settings ---
    print("\n--- Basic Settings ---")
    start_date_str = input("Enter simulation start date (YYYY-MM-DD): ")
    end_date_str = input("Enter simulation end date (YYYY-MM-DD): ")
    delay_threshold = int(input("Enter delay threshold in minutes [default: 15]: ") or "15")
    backlog_threshold = int(input("Enter backlog threshold in flights/hour [default: 10]: ") or "10")

    # --- Flight Data ---
    print("\n--- Flight Plan Data ---")
    print("A: Use default data ('数据/5月航班运行数据（脱敏）.xlsx')")
    print("B: Provide path to a custom .xlsx file")
    data_choice = input("Select an option [A]: ").upper() or 'A'
    if data_choice == 'B':
        flight_data_path = input("Enter the full path to your .xlsx file: ")
    else:
        # 使用默认路径，但现在不添加相对路径，而是在load_and_prepare_data中处理
        flight_data_path = '5月航班运行数据（脱敏）.xlsx'
        
        # 验证默认数据文件是否存在，提供清晰的反馈
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
                # 递归调用以重新开始输入过程
                return get_user_input()
        
    # --- Suspension Periods ---
    print("\n--- Suspension Period Configuration (for weather, etc.) ---")
    suspension_periods = []
    while True:
        add_suspension = input("Add a suspension period? (y/n) [n]: ").lower()
        if add_suspension != 'y':
            break
        print("  Please enter the start and end times (supports multi-day periods):")
        start_datetime_str = input("  Suspension start (YYYY-MM-DD HH:MM): ")
        end_datetime_str = input("  Suspension end (YYYY-MM-DD HH:MM): ")
        
        try:
            start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M")
            end_datetime = datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M")
            
            if end_datetime <= start_datetime:
                print("  Error: End time must be after start time. Please try again.")
                continue
                
            # 创建一个跨天事件的字典
            suspension_period = {
                'start_time': start_datetime,
                'end_time': end_datetime,
                'multi_day': True
            }
            
            # 将事件按日期分解，以便于处理
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
                
            print(f"  Suspension period added from {start_datetime} to {end_datetime}.")
            print(f"  Affecting {len(affected_dates)} day(s).")
            
        except ValueError:
            print("  Invalid date/time format. Use YYYY-MM-DD HH:MM. Please try again.")
            
    # --- Tower Efficiency ---
    print("\n--- Tower Efficiency Configuration ---")
    efficiency_periods = []
    while True:
        add_efficiency = input("Add a low-efficiency period? (y/n) [n]: ").lower()
        if add_efficiency != 'y':
            break
        print("  Please enter the start and end times (supports multi-day periods):")
        start_datetime_str = input("  Low efficiency start (YYYY-MM-DD HH:MM): ")
        end_datetime_str = input("  Low efficiency end (YYYY-MM-DD HH:MM): ")
        
        efficiency_val = float(input("  Efficiency coefficient (0.1-1.0, e.g., 0.8 for 80%): ") or "1.0")
        print("  Impact type:")
        print("  1: Random Delay (affects random flights in the period)")
        print("  2: Sequential Delay (affects all flights in order)")
        print("  3: Priority Delay (large aircraft prioritized, smaller ones delayed)")
        impact_type_choice = input("  Select impact type [2]: ") or '2'
        impact_map = {'1': '随机延误', '2': '按顺序延误', '3': '优先级延误'}
        
        try:
            start_datetime = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M")
            end_datetime = datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M")
            
            if end_datetime <= start_datetime:
                print("  Error: End time must be after start time. Please try again.")
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
                
            print(f"  Low-efficiency period added from {start_datetime} to {end_datetime}.")
            print(f"  Efficiency set to {efficiency_val*100:.0f}%, type: {impact_type}.")
            print(f"  Affecting {len(affected_dates)} day(s).")
            
        except ValueError:
            print("  Invalid date/time or number format. Please try again.")

    return {
        "start_date": datetime.strptime(start_date_str, "%Y-%m-%d"),
        "end_date": datetime.strptime(end_date_str, "%Y-%m-%d"),
        "delay_threshold": delay_threshold,
        "backlog_threshold": backlog_threshold,
        "flight_data_path": flight_data_path,
        "suspension_periods": suspension_periods,
        "efficiency_periods": efficiency_periods,
    }

def main():
    """Main function to run the interactive simulation."""
    try:
        # Collect parameters from user
        params = get_user_input()
        
        # Initialize the simulator
        simulator = ZGGGDepartureSimulator(
            delay_threshold=params['delay_threshold'],
            backlog_threshold=params['backlog_threshold'],
            flight_data_path=params['flight_data_path'],
            suspension_periods=params['suspension_periods'],
            efficiency_periods=params['efficiency_periods']
        )
        
        # Run the simulation steps
        if simulator.load_and_prepare_data():
            simulator.classify_aircraft_types()
            if simulator.run_simulation_for_date_range(params['start_date'], params['end_date']):
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
        print("\n=== 模拟脚本结束 ===")

if __name__ == "__main__":
    main()