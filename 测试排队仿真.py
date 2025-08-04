#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试排队仿真系统 - 创建更复杂的仿真场景
"""

from 机场排队仿真系统 import AirportQueueSimulator

def test_enhanced_simulation():
    """测试增强的仿真场景"""
    print("=== 测试增强排队仿真系统 ===")
    
    # 文件路径
    flight_plans_file = "仿真/flight_plans.xml"
    original_data_file = "数据/三段式飞行- plan所需数据.xlsx"
    output_report_file = "增强版机场排队仿真分析报告.xlsx"
    
    # 创建更具挑战性的仿真器
    simulator = AirportQueueSimulator(
        departure_time=25,  # 增加出港时间到25分钟
        arrival_time=15,    # 增加入港时间到15分钟
        num_runways=1       # 单跑道产生更多排队
    )
    
    # 加载飞行计划
    simulator.load_flight_plans(flight_plans_file)
    
    # 收集机场活动
    airport_activities = simulator.collect_airport_activities()
    
    # 执行仿真，加入更大的随机延误
    print("执行增强仿真（更长时间 + 随机延误）...")
    updated_plans = simulator.simulate_queue(airport_activities, time_disturbance=15)  # 15分钟随机延误
    
    # 更新相关活动
    updated_plans = simulator.update_connected_activities(updated_plans)
    
    # 生成增强分析报告
    simulator.generate_analysis_report(original_data_file, output_report_file)
    
    print("\n=== 增强仿真完成 ===")
    print(f"报告已保存到: {output_report_file}")
    
    # 显示一些有趣的统计
    print("\n=== 仿真结果统计 ===")
    delays = [r['delay_minutes'] for r in simulator.simulation_results]
    if delays:
        import numpy as np
        print(f"总活动数: {len(delays)}")
        print(f"有延误的活动: {sum(1 for d in delays if d > 0)}")
        print(f"平均延误: {np.mean(delays):.2f} 分钟")
        print(f"最大延误: {max(delays):.2f} 分钟")
        print(f"延误比例: {sum(1 for d in delays if d > 0)/len(delays)*100:.1f}%")

if __name__ == "__main__":
    test_enhanced_simulation()
