import pandas as pd
from 机场排队仿真系统 import AirportQueueSimulator

def simple_verification():
    """简单验证修复后的机场解析逻辑"""
    print("=== 简单验证修复后的机场解析逻辑 ===")
    
    simulator = AirportQueueSimulator(
        departure_time=15,
        arrival_time=8,
        num_runways=2
    )
    
    # 加载飞行计划
    simulator.load_flight_plans("仿真/flight_plans.xml")
    
    # 收集机场活动
    airport_activities = simulator.collect_airport_activities()
    
    # 运行仿真
    updated_plans = simulator.simulate_queue(airport_activities)
    
    # 手动创建简化的分析数据
    analysis_data = []
    
    for result in simulator.simulation_results:
        # 获取原始活动信息
        aircraft_id = result['aircraft_id']
        activity_type = result['activity_type']
        airport = result['airport']
        
        # 从flight_plans中找到对应的活动来获取link信息
        aircraft_activities = simulator.flight_plans[aircraft_id]
        original_activity = None
        
        for activity in aircraft_activities:
            if ((activity_type == 'departure' and activity['type'] == 'p') or 
                (activity_type == 'arrival' and activity['type'] == 'd')):
                if ((activity_type == 'departure' and activity['origin'] == airport) or
                    (activity_type == 'arrival' and activity['destination'] == airport)):
                    original_activity = activity
                    break
        
        if original_activity:
            analysis_data.append({
                '飞机ID': aircraft_id,
                '活动类型': '出港' if activity_type == 'departure' else '入港',
                '机场': airport,
                '航线': original_activity['link'],
                '出发机场': original_activity['origin'],
                '到达机场': original_activity['destination'],
                '计划时间': result['scheduled_start'],
                '实际时间': result['actual_start'],
                '延误(分钟)': result['delay_minutes'],
                '跑道': result.get('runway', 1)
            })
    
    # 创建DataFrame并保存
    df = pd.DataFrame(analysis_data)
    
    # 显示前10条记录进行验证
    print("\n=== 修正后的分析结果示例 ===")
    print(df.head(10).to_string(index=False, max_colwidth=15))
    
    # 保存到CSV（避免Excel格式问题）
    df.to_csv("修正后的机场排队分析.csv", index=False, encoding='utf-8-sig')
    print(f"\n✅ 分析结果已保存到: 修正后的机场排队分析.csv")
    
    # 验证关键点
    print("\n=== 关键验证点 ===")
    departure_correct = 0
    arrival_correct = 0
    
    for _, row in df.iterrows():
        if row['活动类型'] == '出港':
            if row['机场'] == row['出发机场']:
                departure_correct += 1
            else:
                print(f"❌ 出港错误: {row['飞机ID']} 在 {row['机场']} 出港，但出发机场是 {row['出发机场']}")
        
        elif row['活动类型'] == '入港':
            if row['机场'] == row['到达机场']:
                arrival_correct += 1
            else:
                print(f"❌ 入港错误: {row['飞机ID']} 在 {row['机场']} 入港，但到达机场是 {row['到达机场']}")
    
    total_departure = len(df[df['活动类型'] == '出港'])
    total_arrival = len(df[df['活动类型'] == '入港'])
    
    print(f"✅ 出港活动验证: {departure_correct}/{total_departure} 正确")
    print(f"✅ 入港活动验证: {arrival_correct}/{total_arrival} 正确")
    
    if departure_correct == total_departure and arrival_correct == total_arrival:
        print("\n🎉 所有机场解析都是正确的！")
    else:
        print("\n⚠️ 仍有部分解析错误需要检查")

if __name__ == "__main__":
    simple_verification()
