import pandas as pd
import numpy as np
from itertools import combinations
import os

def calculate_distance(coord1, coord2):
    """
    计算两个坐标点之间的欧几里得距离
    """
    x1, y1 = coord1
    x2, y2 = coord2
    return np.sqrt((x2 - x1)**2 + (y2 - y1)**2)

def remove_outliers_zscore(data, threshold=2):
    """
    使用Z-score方法剔除异常值
    """
    if len(data) < 3:  # 样本太少不处理
        return data
    
    mean = np.mean(data)
    std = np.std(data)
    
    if std == 0:  # 标准差为0说明所有值相同
        return data
    
    cleaned = [x for x in data if abs(x - mean) <= threshold * std]
    return cleaned if cleaned else data  # 如果全部被剔除，返回原数据

def calculate_route_speeds(flight_data_file):
    """
    计算每条航线的平均速度，剔除异常值
    """
    print("=== 分析航班数据计算航线速度 ===")
    
    if not os.path.exists(flight_data_file):
        print(f"警告: 航班数据文件不存在: {flight_data_file}")
        return {}
    
    try:
        df = pd.read_excel(flight_data_file)
        print(f"读取航班数据: {len(df)} 条记录")
    except Exception as e:
        print(f"读取航班数据出错: {e}")
        return {}
    
    # 检查必要列是否存在
    required_cols = ['计划起飞站四字码', '计划到达站四字码', '实际航程_Mile', '实际起飞时间', '实际落地时间']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"数据文件缺少必要列: {missing_cols}")
        print(f"可用列: {list(df.columns)}")
        return {}
    
    # 安全解析时间函数
    def safe_parse_datetime(time_str):
        if pd.isna(time_str) or time_str == '-' or time_str == '' or str(time_str).strip() == '-':
            return None
        try:
            return pd.to_datetime(str(time_str).strip())
        except:
            return None
    
    # 解析时间数据
    df['起飞时间_dt'] = df['实际起飞时间'].apply(safe_parse_datetime)
    df['落地时间_dt'] = df['实际落地时间'].apply(safe_parse_datetime)
    
    # 过滤有效数据
    valid_mask = (
        df['计划起飞站四字码'].notna() &
        df['计划到达站四字码'].notna() &
        df['实际航程_Mile'].notna() &
        df['起飞时间_dt'].notna() &
        df['落地时间_dt'].notna() &
        (df['实际航程_Mile'] > 0)
    )
    
    df_valid = df[valid_mask].copy()
    print(f"有效数据: {len(df_valid)} 条记录")
    
    # 计算飞行时间（秒）和速度（英里/秒）
    df_valid['飞行时间_秒'] = (df_valid['落地时间_dt'] - df_valid['起飞时间_dt']).dt.total_seconds()
    df_valid = df_valid[df_valid['飞行时间_秒'] > 0]  # 过滤无效飞行时间
    
    df_valid['速度_英里每秒'] = df_valid['实际航程_Mile'] / df_valid['飞行时间_秒']
    
    # 创建航线标识
    df_valid['航线'] = df_valid['计划起飞站四字码'] + '-' + df_valid['计划到达站四字码']
    
    # 按航线分组计算平均速度
    route_speeds = {}
    route_groups = df_valid.groupby('航线')
    
    processed_routes = 0
    total_routes = len(route_groups)
    
    for route, group in route_groups:
        if len(group) < 2:  # 至少需要2个样本
            continue
            
        # 剔除里程异常值
        miles_list = group['实际航程_Mile'].tolist()
        cleaned_miles = remove_outliers_zscore(miles_list, threshold=2)
        
        # 剔除飞行时间异常值
        time_list = group['飞行时间_秒'].tolist()
        cleaned_times = remove_outliers_zscore(time_list, threshold=2)
        
        # 基于清洗后的数据重新计算速度
        if len(cleaned_miles) >= 2 and len(cleaned_times) >= 2:
            # 使用清洗后的平均里程和平均时间计算速度
            avg_miles = np.mean(cleaned_miles)
            avg_time = np.mean(cleaned_times)
            avg_speed_miles_per_sec = avg_miles / avg_time  # 英里/秒
            
            # 转换为米/秒（MATSim标准）
            avg_speed_mps = avg_speed_miles_per_sec * 1609.34  # 1英里 = 1609.34米
            
            route_speeds[route] = avg_speed_mps
            processed_routes += 1
            
            if processed_routes <= 10:  # 显示前10条路线的处理结果
                print(f"  {route}: {len(group)}→{len(cleaned_miles)}条有效, "
                      f"平均{avg_miles:.0f}英里, {avg_time:.0f}秒, {avg_speed_mps:.2f}米/秒")
    
    print(f"成功处理 {processed_routes}/{total_routes} 条航线")
    return route_speeds

def generate_matsim_network(excel_file_path, output_file_path, flight_data_file=None):
    """
    根据航站坐标生成MATSim网络文件，使用真实航线速度
    """
    
    # 读取航站坐标文件
    print(f"正在读取航站坐标文件: {excel_file_path}")
    
    if not os.path.exists(excel_file_path):
        print(f"错误: 文件 {excel_file_path} 不存在")
        return
    
    try:
        df = pd.read_excel(excel_file_path)
        print(f"成功读取数据，共 {len(df)} 个航站")
    except Exception as e:
        print(f"读取文件时出错: {e}")
        return
    
    # 检查必要的列是否存在
    required_columns = ['航站代码', 'X坐标', 'Y坐标']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"缺少以下列: {missing_columns}")
        print(f"实际列名: {list(df.columns)}")
        return
    
    # 清理数据
    df_clean = df.dropna(subset=required_columns)
    print(f"清理后数据: {len(df_clean)} 个航站")
    
    # 获取航站坐标字典
    airports = {}
    for _, row in df_clean.iterrows():
        airport_code = row['航站代码']
        x = row['X坐标']
        y = row['Y坐标']
        airports[airport_code] = (x, y)
    
    print(f"处理 {len(airports)} 个航站")
    
    # 计算航线速度
    route_speeds = {}
    default_speed = 0.12351038 * 1609.34  # 默认速度转换为米/秒（MATSim标准）
    
    if flight_data_file:
        route_speeds = calculate_route_speeds(flight_data_file)
        print(f"获得 {len(route_speeds)} 条航线的真实速度数据")
    else:
        print("未提供航班数据文件，将使用默认速度")
    
    # 开始生成XML文件
    xml_content = []
    xml_content.append('<?xml version="1.0" encoding="utf-8"?>')
    xml_content.append('<!DOCTYPE network SYSTEM "http://www.matsim.org/files/dtd/network_v1.dtd">')
    xml_content.append('')
    xml_content.append('<network name="aviation network">')
    xml_content.append('   <nodes>')
    
    # 添加节点（航站）
    for airport_code, (x, y) in airports.items():
        xml_content.append(f'      <node id="{airport_code}" x="{x}" y="{y}"/>')
    
    xml_content.append('   </nodes>')
    xml_content.append('   <links capperiod="01:00:00">')
    
    # 添加链接（航线）
    airport_list = list(airports.keys())
    link_count = 0
    real_speed_count = 0
    
    for i in range(len(airport_list)):
        for j in range(len(airport_list)):
            if i != j:  # 排除自环
                airport_a = airport_list[i]
                airport_b = airport_list[j]
                
                # 计算距离
                coord_a = airports[airport_a]
                coord_b = airports[airport_b]
                distance = calculate_distance(coord_a, coord_b)
                
                # 创建链接ID和航线标识
                link_id = f"{airport_a}-{airport_b}"
                route_key = f"{airport_a}-{airport_b}"
                
                # 确定速度值
                if route_key in route_speeds:
                    speed = route_speeds[route_key]
                    real_speed_count += 1
                else:
                    speed = default_speed
                
                # 添加链接
                xml_content.append(f'      <link id="{link_id}" from="{airport_a}" to="{airport_b}" length="{distance:.2f}" capacity="10" freespeed="{speed:.2f}" permlanes="1" />')
                link_count += 1
    
    xml_content.append('   </links>')
    xml_content.append('</network>')
    
    # 写入文件
    print(f"\n正在生成网络文件: {output_file_path}")
    print(f"总共生成 {len(airports)} 个节点和 {link_count} 个链接")
    print(f"其中 {real_speed_count} 个链接使用真实速度，{link_count - real_speed_count} 个使用默认速度")
    
    try:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(xml_content))
        print(f"✅ 网络文件生成成功: {output_file_path}")
    except Exception as e:
        print(f"❌ 写入文件时出错: {e}")
    
    # 输出统计信息
    print(f"\n=== 网络统计信息 ===")
    print(f"节点数量: {len(airports)}")
    print(f"链接数量: {link_count}")
    print(f"真实速度链接: {real_speed_count}")
    print(f"默认速度链接: {link_count - real_speed_count}")
    print(f"平均每个节点的出度: {link_count / len(airports):.1f}")
    
    # 计算一些距离统计
    distances = []
    for i in range(len(airport_list)):
        for j in range(i+1, len(airport_list)):
            airport_a = airport_list[i]
            airport_b = airport_list[j]
            coord_a = airports[airport_a]
            coord_b = airports[airport_b]
            distance = calculate_distance(coord_a, coord_b)
            distances.append(distance)
    
    if distances:
        print(f"最短距离: {min(distances):.2f}")
        print(f"最长距离: {max(distances):.2f}")
        print(f"平均距离: {np.mean(distances):.2f}")
        print(f"距离中位数: {np.median(distances):.2f}")
    
    # 速度统计
    if route_speeds:
        speeds = list(route_speeds.values())
        print(f"\n=== 速度统计信息 ===")
        print(f"最低速度: {min(speeds):.2f} 米/秒 ({min(speeds)*3.6:.0f} km/h)")
        print(f"最高速度: {max(speeds):.2f} 米/秒 ({max(speeds)*3.6:.0f} km/h)")
        print(f"平均速度: {np.mean(speeds):.2f} 米/秒 ({np.mean(speeds)*3.6:.0f} km/h)")
        print(f"速度中位数: {np.median(speeds):.2f} 米/秒 ({np.median(speeds)*3.6:.0f} km/h)")
        print(f"默认速度: {default_speed:.2f} 米/秒 ({default_speed*3.6:.0f} km/h)")

def main():
    # 文件路径
    excel_file = "/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/航站坐标.xlsx"
    output_file = "/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/仿真/network_matsim_standard.xml"
    
    # 航班数据文件路径（可选）
    flight_data_file = "/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/5月航班运行数据（脱敏）.xlsx"
    
    # 检查航班数据文件是否存在
    if not os.path.exists(flight_data_file):
        print(f"警告: 航班数据文件不存在，尝试其他数据文件...")
        alternative_file = "/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/三段式飞行- plan所需数据.xlsx"
        if os.path.exists(alternative_file):
            flight_data_file = alternative_file
            print(f"使用备选数据文件: {flight_data_file}")
        else:
            flight_data_file = None
            print("未找到合适的航班数据文件，将使用默认速度")
    
    # 生成网络文件
    generate_matsim_network(excel_file, output_file, flight_data_file)

if __name__ == "__main__":
    main()