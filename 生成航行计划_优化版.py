import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

def load_airport_coordinates(coord_file):
    """
    读取航站坐标文件
    """
    print(f"正在读取航站坐标文件: {coord_file}")
    
    if not os.path.exists(coord_file):
        print(f"错误: 文件 {coord_file} 不存在")
        return None
    
    try:
        df = pd.read_excel(coord_file)
        print(f"成功读取航站坐标，共 {len(df)} 个航站")
    except Exception as e:
        print(f"读取航站坐标文件时出错: {e}")
        return None
    
    # 检查列名
    required_columns = ['航站代码', 'X坐标', 'Y坐标']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"航站坐标文件缺少以下列: {missing_columns}")
        return None
    
    # 创建坐标字典
    coords = {}
    for _, row in df.iterrows():
        airport_code = row['航站代码']
        x = row['X坐标']
        y = row['Y坐标']
        coords[airport_code] = (x, y)
    
    return coords

def load_flight_data(flight_file):
    """
    读取航班数据文件
    """
    print(f"正在读取航班数据文件: {flight_file}")
    
    if not os.path.exists(flight_file):
        print(f"错误: 文件 {flight_file} 不存在")
        return None
    
    try:
        df = pd.read_excel(flight_file)
        print(f"成功读取航班数据，共 {len(df)} 条记录")
    except Exception as e:
        print(f"读取航班数据文件时出错: {e}")
        return None
    
    # 检查必要的列
    required_columns = ['机尾号', '计划起飞站四字码', '计划到达站四字码', '计划离港时间', '计划到港时间', '实际起飞时间', '实际落地时间']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"航班数据文件缺少以下列: {missing_columns}")
        print(f"实际列名: {list(df.columns)}")
        return None
    
    # 数据清洗
    df_clean = df.dropna(subset=required_columns)
    
    # 进一步过滤数据：排除机尾号为"-"或时刻信息为"-"的记录
    def is_valid_data(row):
        # 检查机尾号
        if pd.isna(row['机尾号']) or str(row['机尾号']).strip() == "-":
            return False
        
        # 检查时刻信息
        time_columns = ['计划离港时间', '计划到港时间', '实际起飞时间', '实际落地时间']
        for col in time_columns:
            value = row[col]
            if pd.isna(value) or str(value).strip() == "-" or str(value).strip() == "":
                return False
        
        return True
    
    # 应用过滤条件
    valid_mask = df_clean.apply(is_valid_data, axis=1)
    df_filtered = df_clean[valid_mask]
    
    print(f"过滤无效数据后: {len(df_filtered)} 条记录")
    print(f"被过滤的记录数: {len(df_clean) - len(df_filtered)}")
    
    return df_filtered

def parse_time(time_str):
    """
    解析时间字符串为MATSim格式，从2025-05-01 00:00:00开始累积计算
    """
    try:
        if pd.isna(time_str):
            return "06:00:00"
        
        # 基准时间：2025-05-01 00:00:00
        base_time = datetime(2025, 5, 1, 0, 0, 0)
        
        # 如果是字符串格式，尝试解析
        if isinstance(time_str, str):
            # 尝试解析完整的日期时间格式
            try:
                # 尝试多种可能的格式
                formats = [
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M",
                    "%Y/%m/%d %H:%M:%S",
                    "%Y/%m/%d %H:%M",
                    "%m/%d/%Y %H:%M:%S",
                    "%m/%d/%Y %H:%M",
                    "%H:%M:%S",
                    "%H:%M"
                ]
                
                parsed_time = None
                for fmt in formats:
                    try:
                        parsed_time = datetime.strptime(time_str, fmt)
                        break
                    except ValueError:
                        continue
                
                if parsed_time is None:
                    print(f"无法解析时间格式: {time_str}")
                    return "06:00:00"
                
                # 如果只是时间部分（没有日期），假设是2025-05-01
                if parsed_time.year == 1900:  # strptime默认年份
                    parsed_time = parsed_time.replace(year=2025, month=5, day=1)
                
            except Exception as e:
                print(f"解析时间字符串时出错: {time_str}, 错误: {e}")
                return "06:00:00"
        
        # 如果是datetime对象
        elif isinstance(time_str, datetime):
            parsed_time = time_str
        
        # 如果是pandas时间戳
        elif hasattr(time_str, 'to_pydatetime'):
            parsed_time = time_str.to_pydatetime()
        
        # 如果有strftime方法但不是datetime
        elif hasattr(time_str, 'strftime'):
            try:
                parsed_time = datetime.strptime(time_str.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            except:
                return "06:00:00"
        
        else:
            print(f"未知的时间格式: {time_str}, 类型: {type(time_str)}")
            return "06:00:00"
        
        # 计算从基准时间开始的累积小时数
        time_diff = parsed_time - base_time
        total_seconds = int(time_diff.total_seconds())
        
        # 如果时间早于基准时间，设置为基准时间
        if total_seconds < 0:
            total_seconds = 0
        
        # 计算累积的小时、分钟、秒
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        # 格式化为HH:MM:SS（小时可以超过24）
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    
    except Exception as e:
        print(f"解析时间时出错: {time_str}, 错误: {e}")
        return "06:00:00"

def filter_by_time_range(flight_data, start_time=None, end_time=None):
    """
    根据时间范围过滤航班数据
    """
    if start_time is None:
        start_time = datetime(2025, 5, 1, 0, 0, 0)  # 默认开始时间
    if end_time is None:
        end_time = datetime(2025, 6, 1, 0, 0, 0)    # 默认结束时间
    
    print(f"筛选时间范围: {start_time.strftime('%Y-%m-%d %H:%M:%S')} 到 {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    def is_in_time_range(row):
        """检查航班是否在指定时间范围内"""
        try:
            # 解析实际起飞时间
            departure_time_str = row['实际起飞时间']
            
            # 如果是字符串格式，尝试解析
            if isinstance(departure_time_str, str):
                formats = [
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M",
                    "%Y/%m/%d %H:%M:%S",
                    "%Y/%m/%d %H:%M",
                    "%m/%d/%Y %H:%M:%S",
                    "%m/%d/%Y %H:%M",
                ]
                
                parsed_time = None
                for fmt in formats:
                    try:
                        parsed_time = datetime.strptime(departure_time_str, fmt)
                        break
                    except ValueError:
                        continue
                
                if parsed_time is None:
                    return False
                    
            elif isinstance(departure_time_str, datetime):
                parsed_time = departure_time_str
            elif hasattr(departure_time_str, 'to_pydatetime'):
                parsed_time = departure_time_str.to_pydatetime()
            else:
                return False
            
            # 检查是否在时间范围内
            return start_time <= parsed_time <= end_time
            
        except Exception as e:
            print(f"处理时间数据出错: {e}")
            return False
    
    # 应用时间过滤
    time_mask = flight_data.apply(is_in_time_range, axis=1)
    filtered_data = flight_data[time_mask]
    
    print(f"时间过滤后: {len(filtered_data)} 条记录 (原始: {len(flight_data)} 条)")
    
    return filtered_data

def create_flight_plans(flight_data, airport_coords, max_aircraft=None, start_time=None, end_time=None):
    """
    根据航班数据创建三段式飞行计划
    
    参数:
    - flight_data: 航班数据
    - airport_coords: 航站坐标
    - max_aircraft: 最大飞机数量，None表示全部飞机
    - start_time: 开始时间，None使用默认值
    - end_time: 结束时间，None使用默认值
    """
    print("正在生成三段式飞行计划...")
    
    # 首先根据时间范围过滤数据
    flight_data_filtered = filter_by_time_range(flight_data, start_time, end_time)
    
    if len(flight_data_filtered) == 0:
        print("在指定时间范围内没有找到任何航班数据")
        return []
    
    # 按机尾号分组
    planes = flight_data_filtered.groupby('机尾号')
    
    # 控制飞机数量
    if max_aircraft is not None and isinstance(max_aircraft, int) and max_aircraft > 0:
        # 选择前N个飞机
        plane_ids = list(planes.groups.keys())[:max_aircraft]
        print(f"选择前 {max_aircraft} 架飞机: {plane_ids}")
        planes = {plane_id: planes.get_group(plane_id) for plane_id in plane_ids}
    elif max_aircraft is not None and isinstance(max_aircraft, list):
        # 选择指定的飞机
        available_planes = set(planes.groups.keys())
        selected_planes = [plane_id for plane_id in max_aircraft if plane_id in available_planes]
        print(f"选择指定飞机: {selected_planes}")
        if not selected_planes:
            print("警告: 指定的飞机都不在数据中")
            return []
        planes = {plane_id: planes.get_group(plane_id) for plane_id in selected_planes}
    else:
        # 使用所有飞机
        print(f"使用所有飞机，共 {len(planes)} 架")
        planes = {plane_id: group for plane_id, group in planes}
    
    plans = []
    
    for plane_id, plane_flights in planes.items():
        # 按实际起飞时间排序
        plane_flights = plane_flights.sort_values('实际起飞时间')
        
        if len(plane_flights) == 0:
            continue
        
        print(f"处理飞机 {plane_id}，共 {len(plane_flights)} 个航班")
        
        # 创建person计划，使用机尾号作为ID
        person_plan = {
            'id': str(plane_id),
            'activities': [],
            'legs': []
        }
        
        # 处理每个航班
        for idx, (_, flight) in enumerate(plane_flights.iterrows()):
            departure_airport = flight['计划起飞站四字码']
            arrival_airport = flight['计划到达站四字码']
            
            # 解析各种时间
            planned_departure_time = parse_time(flight['计划离港时间'])
            planned_arrival_time = parse_time(flight['计划到港时间'])
            actual_departure_time = parse_time(flight['实际起飞时间'])
            actual_landing_time = parse_time(flight['实际落地时间'])
            
            # 检查航站坐标是否存在
            if departure_airport not in airport_coords:
                print(f"警告: 航站 {departure_airport} 坐标不存在，跳过")
                continue
            
            if arrival_airport not in airport_coords:
                print(f"警告: 航站 {arrival_airport} 坐标不存在，跳过")
                continue
            
            dep_x, dep_y = airport_coords[departure_airport]
            arr_x, arr_y = airport_coords[arrival_airport]
            
            # 如果不是第一个航班，需要添加等待活动
            if idx > 0:
                # 获取上一次降落的结束时间
                last_landing_end_time = person_plan['activities'][-1]['end_time']
                
                # 添加等待活动 (type=w) - 在当前出发机场等待
                wait_activity = {
                    'type': 'w',
                    'x': dep_x,
                    'y': dep_y,
                    'link': f"{departure_airport}-{arrival_airport}",
                    'start_time': last_landing_end_time,
                    'end_time': planned_departure_time
                }
                person_plan['activities'].append(wait_activity)
                
                # 添加瞬时leg（从等待位置到准备位置）
                wait_to_prep_leg = {
                    'mode': 'car',
                    'trav_time': '00:00:00'
                }
                person_plan['legs'].append(wait_to_prep_leg)
            
            # 1. 准备活动 (type=p) - 在A点，从计划离港时间到实际起飞时间
            prep_activity = {
                'type': 'p',
                'x': dep_x,
                'y': dep_y,
                'link': f"{departure_airport}-{arrival_airport}",
                'start_time': planned_departure_time,
                'end_time': actual_departure_time
            }
            person_plan['activities'].append(prep_activity)
            
            # 2. 瞬时leg - 距离为0的出行
            instant_leg_1 = {
                'mode': 'car',  # 使用car模式表示瞬时移动
                'trav_time': '00:00:00'  # 瞬时完成
            }
            person_plan['legs'].append(instant_leg_1)
            
            # 3. 飞行准备活动 (type=f) - 在A点，瞬时完成
            flight_prep_activity = {
                'type': 'f',
                'x': dep_x,
                'y': dep_y,
                'link': f"{departure_airport}-{arrival_airport}",
                'end_time': actual_departure_time  # 瞬时完成，end时间等于start时间
            }
            person_plan['activities'].append(flight_prep_activity)
            
            # 4. 真实飞行leg - 从A点到B点的航线飞行
            flight_leg = {
                'mode': 'car',  # 使用car模式表示飞行
                'dep_time': actual_departure_time,
                'trav_time': None  # 让MATSim自动计算飞行时间
            }
            person_plan['legs'].append(flight_leg)
            
            # 5. 降落活动 (type=d) - 在B点，从实际落地时间到计划到港时间
            landing_activity = {
                'type': 'd',
                'x': arr_x,
                'y': arr_y,
                'link': f"{arrival_airport}-{departure_airport}",
                'start_time': actual_landing_time,
                'end_time': planned_arrival_time
            }
            person_plan['activities'].append(landing_activity)
            
            # 6. 如果不是最后一个航班，添加瞬时leg为下一轮做准备
            if idx < len(plane_flights) - 1:
                instant_leg_2 = {
                    'mode': 'car',  # 瞬时移动到下一个航班
                    'trav_time': '00:00:00'
                }
                person_plan['legs'].append(instant_leg_2)
        
        plans.append(person_plan)
    
    print(f"生成了 {len(plans)} 个飞行计划")
    return plans

def generate_matsim_plans_xml(plans, output_file):
    """
    生成MATSim格式的XML文件
    """
    print(f"正在生成MATSim计划文件: {output_file}")
    
    xml_content = []
    xml_content.append('<?xml version="1.0" ?>')
    xml_content.append('<!DOCTYPE plans SYSTEM "http://www.matsim.org/files/dtd/plans_v4.dtd">')
    xml_content.append('<plans xml:lang="de-CH">')
    
    for plan in plans:
        xml_content.append(f'<person id="{plan["id"]}">')
        xml_content.append('\t<plan>')
        
        # 交替添加活动和leg
        for i, activity in enumerate(plan['activities']):
            # 添加活动
            act_line = f'\t\t<act type="{activity["type"]}" x="{activity["x"]}" y="{activity["y"]}" link="{activity["link"]}"'
            
            if 'start_time' in activity and 'end_time' in activity:
                act_line += f' start_time="{activity["start_time"]}" end_time="{activity["end_time"]}"'
            elif 'end_time' in activity:
                act_line += f' end_time="{activity["end_time"]}"'
            elif 'dur' in activity:
                act_line += f' dur="{activity["dur"]}"'
            
            act_line += ' />'
            xml_content.append(act_line)
            
            # 添加leg（除了最后一个活动）
            if i < len(plan['legs']):
                leg = plan['legs'][i]
                leg_line = f'\t\t<leg mode="{leg["mode"]}"'
                
                # 添加leg的属性
                if 'dep_time' in leg:
                    leg_line += f' dep_time="{leg["dep_time"]}"'
                if 'trav_time' in leg and leg['trav_time'] is not None:
                    leg_line += f' trav_time="{leg["trav_time"]}"'
                
                leg_line += '>'
                xml_content.append(leg_line)
                xml_content.append('\t\t</leg>')
        
        xml_content.append('\t</plan>')
        xml_content.append('</person>')
        xml_content.append('')
    
    xml_content.append('</plans>')
    
    # 写入文件
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(xml_content))
        print(f"✅ MATSim计划文件生成成功: {output_file}")
    except Exception as e:
        print(f"❌ 写入文件时出错: {e}")

def main():
    """
    主函数
    
    新的飞行计划结构说明:
    第一个航班:
    1. p (准备活动): 在A点，从计划离港时间到实际起飞时间
    2. leg (瞬时): 距离为0的瞬时出行 
    3. f (飞行准备): 在A点，瞬时完成（end=实际起飞时间）
    4. leg (真实飞行): 从A点到B点的航线飞行
    5. d (降落活动): 在B点，从实际落地时间到计划到港时间
    
    后续航班（在前面增加等待环节）:
    0. w (等待活动): 在A点，从上次降落结束到本次计划离港时间
    1. leg (瞬时): 从等待到准备的瞬时移动
    2. p (准备活动): 在A点，从计划离港时间到实际起飞时间
    3. leg (瞬时): 距离为0的瞬时出行
    4. f (飞行准备): 在A点，瞬时完成（end=实际起飞时间）
    5. leg (真实飞行): 从A点到B点的航线飞行
    6. d (降落活动): 在B点，从实际落地时间到计划到港时间
    """
    # ==================== 配置参数 ====================
    # 时间范围控制
    START_TIME = datetime(2025, 5, 1, 0, 0, 0)   # 开始时间：5月1日00:00:00
    END_TIME = datetime(2025, 6, 1, 0, 0, 0)     # 结束时间：6月1日00:00:00

    # 飞机数量控制
    # MAX_AIRCRAFT = None              # None表示全部飞机
    # MAX_AIRCRAFT = 5                 # 整数表示选择前N架飞机
    # MAX_AIRCRAFT = ['B-1234', 'B-5678']  # 列表表示选择指定的飞机
    MAX_AIRCRAFT = None  # 暂定选择前1架飞机进行测试
    
    print("=== 飞行计划生成配置 ===")
    print(f"时间范围: {START_TIME.strftime('%Y-%m-%d %H:%M:%S')} 至 {END_TIME.strftime('%Y-%m-%d %H:%M:%S')}")
    if MAX_AIRCRAFT is None:
        print("飞机选择: 全部飞机")
    elif isinstance(MAX_AIRCRAFT, int):
        print(f"飞机选择: 前 {MAX_AIRCRAFT} 架飞机")
    elif isinstance(MAX_AIRCRAFT, list):
        print(f"飞机选择: 指定飞机 {MAX_AIRCRAFT}")
    print("=" * 40)
    
    # 文件路径
    flight_data_file = "/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/三段式飞行- plan所需数据.xlsx"
    coord_file = "/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/航站坐标.xlsx"
    output_file = "/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/仿真/all_flight_plans.xml"
    
    # 读取航站坐标
    airport_coords = load_airport_coordinates(coord_file)
    if airport_coords is None:
        print("无法读取航站坐标，程序退出")
        return
    
    # 读取航班数据
    flight_data = load_flight_data(flight_data_file)
    if flight_data is None:
        print("无法读取航班数据，程序退出")
        return
    
    # 创建飞行计划（传入时间和飞机控制参数）
    plans = create_flight_plans(flight_data, airport_coords, MAX_AIRCRAFT, START_TIME, END_TIME)
    if not plans:
        print("没有生成任何飞行计划，程序退出")
        return
    
    # 生成XML文件
    generate_matsim_plans_xml(plans, output_file)
    
    print("\n=== 处理完成 ===")
    print(f"时间范围: {START_TIME.strftime('%Y-%m-%d %H:%M:%S')} 至 {END_TIME.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"生成的飞行计划数量: {len(plans)}")
    print(f"输出文件: {output_file}")

if __name__ == "__main__":
    main()
