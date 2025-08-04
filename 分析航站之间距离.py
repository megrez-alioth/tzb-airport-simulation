import pandas as pd
import numpy as np
from itertools import combinations
import os

def analyze_airport_connections(excel_file_path):
    """
    分析航站之间的连接关系和距离矩阵
    
    Args:
        excel_file_path (str): Excel文件路径
    
    Returns:
        tuple: (完整距离矩阵, 所有航站列表, 连接状态)
    """
    
    # 读取Excel文件
    print(f"正在读取文件: {excel_file_path}")
    try:
        df = pd.read_excel(excel_file_path)
        print(f"成功读取数据，共 {len(df)} 条记录")
    except Exception as e:
        print(f"读取文件时出错: {e}")
        return None, None, False
    
    # 检查列名是否存在
    required_columns = ['实际起飞站四字码', '实际到达站四字码', '实际航程_Mile']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"缺少以下列: {missing_columns}")
        print(f"实际列名: {list(df.columns)}")
        return None, None, False
    
    # 数据清洗：去除空值
    df_clean = df.dropna(subset=required_columns)
    print(f"清洗后数据：{len(df_clean)} 条记录")
    
    # 获取所有航站
    origin_airports = set(df_clean['实际起飞站四字码'].unique())
    destination_airports = set(df_clean['实际到达站四字码'].unique())
    all_airports = sorted(list(origin_airports.union(destination_airports)))
    
    print(f"总共发现 {len(all_airports)} 个航站:")
    for i, airport in enumerate(all_airports, 1):
        print(f"{i:2d}. {airport}")
    
    # 创建距离字典
    distance_dict = {}
    
    # 遍历数据，建立距离映射
    for _, row in df_clean.iterrows():
        origin = row['实际起飞站四字码']
        destination = row['实际到达站四字码']
        distance = row['实际航程_Mile']
        
        # 存储双向距离（假设航程是对称的）
        distance_dict[(origin, destination)] = distance
        distance_dict[(destination, origin)] = distance
    
    # 创建距离矩阵
    n = len(all_airports)
    distance_matrix = np.full((n, n), np.inf)
    
    # 填充距离矩阵
    for i, airport_i in enumerate(all_airports):
        for j, airport_j in enumerate(all_airports):
            if i == j:
                distance_matrix[i][j] = 0
            elif (airport_i, airport_j) in distance_dict:
                distance_matrix[i][j] = distance_dict[(airport_i, airport_j)]
    
    # 创建DataFrame形式的距离矩阵
    distance_df = pd.DataFrame(distance_matrix, 
                              index=all_airports, 
                              columns=all_airports)
    
    # 检查连接完整性
    print("\n=== 连接完整性分析 ===")
    
    # 计算所有可能的航站对
    total_pairs = len(all_airports) * (len(all_airports) - 1)
    connected_pairs = 0
    missing_connections = []
    
    for i, airport_i in enumerate(all_airports):
        for j, airport_j in enumerate(all_airports):
            if i != j:
                if distance_matrix[i][j] != np.inf:
                    connected_pairs += 1
                else:
                    missing_connections.append((airport_i, airport_j))
    
    connection_rate = connected_pairs / total_pairs * 100
    
    print(f"总航站对数: {total_pairs}")
    print(f"已连接航站对数: {connected_pairs}")
    print(f"连接率: {connection_rate:.2f}%")
    
    is_fully_connected = len(missing_connections) == 0
    
    if is_fully_connected:
        print("✅ 所有航站之间都有连接！")
    else:
        print(f"❌ 缺少 {len(missing_connections)} 个连接")
        print("\n缺少的连接:")
        for i, (origin, dest) in enumerate(missing_connections[:20]):  # 只显示前20个
            print(f"{i+1:2d}. {origin} -> {dest}")
        if len(missing_connections) > 20:
            print(f"... 还有 {len(missing_connections) - 20} 个缺少的连接")
    
    # 统计信息
    print("\n=== 统计信息 ===")
    finite_distances = distance_matrix[distance_matrix != np.inf]
    finite_distances = finite_distances[finite_distances != 0]
    
    if len(finite_distances) > 0:
        print(f"最短航程: {finite_distances.min():.2f} 英里")
        print(f"最长航程: {finite_distances.max():.2f} 英里")
        print(f"平均航程: {finite_distances.mean():.2f} 英里")
        print(f"航程中位数: {np.median(finite_distances):.2f} 英里")
    
    return distance_df, all_airports, is_fully_connected, missing_connections

def save_distance_matrix(distance_df, output_path):
    """
    保存距离矩阵到Excel文件
    """
    print(f"\n正在保存距离矩阵到: {output_path}")
    
    # 将inf替换为空字符串以便更好地显示
    distance_df_display = distance_df.copy()
    distance_df_display = distance_df_display.replace(np.inf, '')
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        distance_df_display.to_excel(writer, sheet_name='距离矩阵')
        print("✅ 距离矩阵已保存")

def main():
    # 文件路径
    excel_file = "数据/5月航班运行数据（实际数据列）.xlsx"
    
    # 检查文件是否存在
    if not os.path.exists(excel_file):
        print(f"错误: 文件 {excel_file} 不存在")
        return
    
    print("=" * 50)
    print("航站连接关系与距离矩阵分析")
    print("=" * 50)
    
    # 分析数据
    distance_df, all_airports, is_fully_connected, missing_connections = analyze_airport_connections(excel_file)
    
    if distance_df is not None:
        # 保存结果
        output_file = "航站距离矩阵.xlsx"
        save_distance_matrix(distance_df, output_file)
        
        # 显示部分距离矩阵
        print("\n=== 距离矩阵（前10x10）===")
        print(distance_df.iloc[:10, :10])
        
        if not is_fully_connected:
            print(f"\n注意: 距离矩阵中的空值表示该航站对之间没有直接连接")
            print(f"共有 {len(missing_connections)} 个航站对缺少连接")
        
        print(f"\n完整的距离矩阵已保存到: {output_file}")
    
    print("\n分析完成！")

if __name__ == "__main__":
    main()