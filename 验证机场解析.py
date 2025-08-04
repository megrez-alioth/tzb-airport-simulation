import pandas as pd
import xml.etree.ElementTree as ET
from 机场排队仿真系统 import AirportQueueSimulator

def test_airport_parsing():
    """测试机场解析逻辑是否正确"""
    print("=== 测试机场解析逻辑 ===")
    
    # 使用真实的flight_plans.xml文件
    xml_file = "仿真/flight_plans.xml"
    
    simulator = AirportQueueSimulator(
        departure_time=20,
        arrival_time=10,
        num_runways=2
    )
    
    # 加载飞行计划
    simulator.load_flight_plans(xml_file)
    
    # 显示前几个飞机的活动解析结果
    print("\n=== 飞行计划解析结果验证 ===")
    count = 0
    for aircraft_id, activities in simulator.flight_plans.items():
        if count >= 3:  # 只显示前3架飞机
            break
            
        print(f"\n飞机 {aircraft_id}:")
        for i, activity in enumerate(activities):
            if activity['type'] in ['p', 'd']:  # 只显示出港准备和入港降落
                print(f"  活动{i}: {activity['type']} | link={activity['link']} | "
                      f"origin={activity['origin']} | destination={activity['destination']}")
                
                if activity['type'] == 'p':
                    print(f"    -> 出港准备在 {activity['origin']} 机场")
                elif activity['type'] == 'd':
                    print(f"    -> 入港降落在 {activity['destination']} 机场")
        count += 1
    
    # 收集机场活动
    airport_activities = simulator.collect_airport_activities()
    
    # 显示机场活动统计
    print("\n=== 机场活动统计 ===")
    for airport, activities in airport_activities.items():
        dep_count = len(activities['departure'])
        arr_count = len(activities['arrival'])
        if dep_count > 0 or arr_count > 0:
            print(f"{airport}: {dep_count}次出港, {arr_count}次入港")
    
    # 详细显示某个机场的活动
    if 'ZGGG' in airport_activities:
        print(f"\n=== ZGGG机场详细活动 ===")
        zggg_activities = airport_activities['ZGGG']
        
        if zggg_activities['departure']:
            print("出港活动:")
            for i, dep in enumerate(zggg_activities['departure'][:5]):  # 显示前5个
                activity = dep['activity']
                print(f"  {dep['aircraft_id']}: {activity['type']} | {activity['link']} | "
                      f"时间: {activity['start_time']}")
        
        if zggg_activities['arrival']:
            print("入港活动:")
            for i, arr in enumerate(zggg_activities['arrival'][:5]):  # 显示前5个
                activity = arr['activity']
                print(f"  {arr['aircraft_id']}: {activity['type']} | {activity['link']} | "
                      f"时间: {activity['start_time']}")

def run_corrected_simulation():
    """运行修正后的仿真"""
    print("\n=== 运行修正后的排队仿真 ===")
    
    simulator = AirportQueueSimulator(
        departure_time=15,
        arrival_time=8,
        num_runways=2
    )
    
    # 加载真实飞行计划
    simulator.load_flight_plans("仿真/flight_plans.xml")
    
    # 收集机场活动
    airport_activities = simulator.collect_airport_activities()
    
    # 运行仿真
    updated_plans = simulator.simulate_queue(airport_activities, time_disturbance=2)
    
    # 生成分析报告
    simulator.generate_analysis_report("仿真/flight_plans.xml", "修正后的机场排队仿真分析报告.xlsx")
    
    print("\n修正后的仿真完成！请检查生成的Excel报告中的机场信息是否正确。")

if __name__ == "__main__":
    # 测试机场解析逻辑
    test_airport_parsing()
    
    # 运行修正后的仿真
    run_corrected_simulation()
