import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import numpy as np
from collections import defaultdict
import random

def create_conflict_scenario():
    """创建一个有排队冲突的测试场景"""
    print("=== 创建排队冲突测试场景 ===")
    
    # 创建多架飞机在同一机场同时起降的场景
    conflict_plans = {
        'TEST001': [
            {'type': 'p', 'link': 'ZBAA-ZSPD', 'start_time': '08:00:00', 'end_time': '08:20:00', 'x': 100, 'y': 200},
            {'type': 'f', 'link': 'ZBAA-ZSPD', 'end_time': '08:20:00', 'x': 100, 'y': 200},
            {'type': 'd', 'link': 'ZSPD-ZBAA', 'start_time': '10:30:00', 'end_time': '10:40:00', 'x': 300, 'y': 50}
        ],
        'TEST002': [
            {'type': 'p', 'link': 'ZBAA-ZGGG', 'start_time': '08:05:00', 'end_time': '08:25:00', 'x': 100, 'y': 200},
            {'type': 'f', 'link': 'ZBAA-ZGGG', 'end_time': '08:25:00', 'x': 100, 'y': 200},
            {'type': 'd', 'link': 'ZGGG-ZBAA', 'start_time': '11:00:00', 'end_time': '11:10:00', 'x': -356, 'y': 49}
        ],
        'TEST003': [
            {'type': 'p', 'link': 'ZBAA-ZSSS', 'start_time': '08:10:00', 'end_time': '08:30:00', 'x': 100, 'y': 200},
            {'type': 'f', 'link': 'ZBAA-ZSSS', 'end_time': '08:30:00', 'x': 100, 'y': 200},
            {'type': 'd', 'link': 'ZSSS-ZBAA', 'start_time': '10:35:00', 'end_time': '10:45:00', 'x': 90, 'y': -508}
        ],
        'TEST004': [
            {'type': 'd', 'link': 'ZSPD-ZBAA', 'start_time': '08:15:00', 'end_time': '08:25:00', 'x': 100, 'y': 200},
            {'type': 'w', 'link': 'ZSPD-ZBAA', 'start_time': '08:25:00', 'end_time': '10:00:00', 'x': 100, 'y': 200},
            {'type': 'p', 'link': 'ZBAA-ZSPD', 'start_time': '10:00:00', 'end_time': '10:20:00', 'x': 100, 'y': 200}
        ],
        'TEST005': [
            {'type': 'd', 'link': 'ZGGG-ZBAA', 'start_time': '08:20:00', 'end_time': '08:30:00', 'x': 100, 'y': 200},
            {'type': 'w', 'link': 'ZGGG-ZBAA', 'start_time': '08:30:00', 'end_time': '10:30:00', 'x': 100, 'y': 200},
            {'type': 'p', 'link': 'ZBAA-ZGGG', 'start_time': '10:30:00', 'end_time': '10:50:00', 'x': 100, 'y': 200}
        ]
    }
    
    return conflict_plans

def test_queue_simulation():
    """测试排队仿真系统"""
    from 机场排队仿真系统 import AirportQueueSimulator
    
    print("=== 测试排队仿真系统 ===")
    
    # 创建仿真器
    simulator = AirportQueueSimulator(
        departure_time=20,  # 出港时间20分钟
        arrival_time=10,    # 入港时间10分钟
        num_runways=1       # 每个机场1条跑道
    )
    
    # 创建测试数据
    conflict_plans = create_conflict_scenario()
    
    # 转换时间格式
    for aircraft_id, activities in conflict_plans.items():
        for activity in activities:
            if activity.get('start_time'):
                activity['start_minutes'] = simulator.parse_time_string(activity['start_time'])
            if activity.get('end_time'):
                activity['end_minutes'] = simulator.parse_time_string(activity['end_time'])
            
            # 解析航站信息
            if activity.get('link') and '-' in activity['link']:
                airports = activity['link'].split('-')
                activity['origin'] = airports[0]
                activity['destination'] = airports[1]
            else:
                activity['origin'] = ''
                activity['destination'] = ''
    
    # 设置测试飞行计划
    simulator.flight_plans = conflict_plans
    
    # 收集机场活动
    airport_activities = simulator.collect_airport_activities()
    
    # 执行排队仿真（加入5分钟的随机延误）
    print("\n执行排队仿真（含随机延误）...")
    updated_plans = simulator.simulate_queue(airport_activities, time_disturbance=5)
    
    # 显示结果
    print("\n=== 仿真结果分析 ===")
    for result in simulator.simulation_results:
        if result['delay_minutes'] > 0:
            print(f"⚠️  {result['aircraft_id']} 在 {result['airport']} "
                  f"{result['activity_type']} 延误 {result['delay_minutes']:.1f} 分钟 "
                  f"(排队第{result['queue_position']}位)")
    
    # 统计延误情况
    total_delays = [r['delay_minutes'] for r in simulator.simulation_results]
    if total_delays:
        print(f"\n📊 延误统计:")
        print(f"   总活动数: {len(total_delays)}")
        print(f"   延误活动数: {sum(1 for d in total_delays if d > 0)}")
        print(f"   平均延误: {np.mean(total_delays):.1f} 分钟")
        print(f"   最大延误: {max(total_delays):.1f} 分钟")
    
    return simulator

def demonstrate_different_scenarios():
    """演示不同的排队场景"""
    from 机场排队仿真系统 import AirportQueueSimulator
    
    print("\n=== 不同场景对比测试 ===")
    
    scenarios = [
        {"name": "标准场景", "dep_time": 20, "arr_time": 10, "disturbance": 0},
        {"name": "快速处理", "dep_time": 15, "arr_time": 8, "disturbance": 0},
        {"name": "延误场景", "dep_time": 25, "arr_time": 12, "disturbance": 3},
        {"name": "高效机场", "dep_time": 10, "arr_time": 5, "disturbance": -2}
    ]
    
    conflict_plans = create_conflict_scenario()
    
    for scenario in scenarios:
        print(f"\n--- {scenario['name']} ---")
        
        simulator = AirportQueueSimulator(
            departure_time=scenario['dep_time'],
            arrival_time=scenario['arr_time'],
            num_runways=1
        )
        
        # 转换时间格式
        test_plans = {}
        for aircraft_id, activities in conflict_plans.items():
            test_plans[aircraft_id] = []
            for activity in activities:
                new_activity = activity.copy()
                if activity.get('start_time'):
                    new_activity['start_minutes'] = simulator.parse_time_string(activity['start_time'])
                if activity.get('end_time'):
                    new_activity['end_minutes'] = simulator.parse_time_string(activity['end_time'])
                
                # 解析航站信息
                if activity.get('link') and '-' in activity['link']:
                    airports = activity['link'].split('-')
                    new_activity['origin'] = airports[0]
                    new_activity['destination'] = airports[1]
                else:
                    new_activity['origin'] = ''
                    new_activity['destination'] = ''
                
                test_plans[aircraft_id].append(new_activity)
        
        simulator.flight_plans = test_plans
        
        # 仿真
        airport_activities = simulator.collect_airport_activities()
        updated_plans = simulator.simulate_queue(airport_activities, 
                                               time_disturbance=scenario['disturbance'])
        
        # 统计结果
        delays = [r['delay_minutes'] for r in simulator.simulation_results]
        delayed_count = sum(1 for d in delays if d > 0)
        
        print(f"   延误活动: {delayed_count}/{len(delays)}")
        print(f"   平均延误: {np.mean(delays):.1f} 分钟")
        print(f"   最大延误: {max(delays) if delays else 0:.1f} 分钟")

if __name__ == "__main__":
    # 运行测试
    simulator = test_queue_simulation()
    
    # 演示不同场景
    demonstrate_different_scenarios()
    
    print("\n=== 测试完成 ===")
