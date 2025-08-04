import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
from collections import defaultdict

def load_flight_data(excel_file_path):
    """
    从Excel文件读取航班数据，计算各航站对之间的平均里程
    """
    print(f"正在读取文件: {excel_file_path}")
    
    # 检查文件是否存在
    if not os.path.exists(excel_file_path):
        print(f"错误: 文件 {excel_file_path} 不存在")
        return None, None
    
    try:
        # 读取Excel文件
        df = pd.read_excel(excel_file_path)
        print(f"成功读取数据，共 {len(df)} 条记录")
    except Exception as e:
        print(f"读取文件时出错: {e}")
        return None, None
    
    # 检查列名是否存在
    required_columns = ['实际起飞站四字码', '实际到达站四字码', '实际航程_Mile']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"缺少以下列: {missing_columns}")
        print(f"实际列名: {list(df.columns)}")
        return None, None
    
    # 数据清洗：去除空值和无效数据
    df_clean = df.dropna(subset=required_columns)
    df_clean = df_clean[df_clean['实际航程_Mile'] > 0]  # 去除负数或零距离
    print(f"清洗后数据：{len(df_clean)} 条记录")
    
    # 收集所有航站对的里程数据
    distance_data = defaultdict(list)
    
    for _, row in df_clean.iterrows():
        origin = row['实际起飞站四字码']
        destination = row['实际到达站四字码']
        distance = row['实际航程_Mile']
        
        # 确保航站对的顺序一致（字母顺序）
        if origin != destination:  # 排除自环
            airport_pair = tuple(sorted([origin, destination]))
            distance_data[airport_pair].append(distance)
    
    # 计算平均里程
    avg_distances = {}
    for pair, distances in distance_data.items():
        avg_distances[pair] = np.mean(distances)
    
    # 获取所有航站
    all_airports = set()
    for pair in avg_distances.keys():
        all_airports.update(pair)
    
    all_airports = sorted(list(all_airports))
    
    print(f"发现 {len(all_airports)} 个航站")
    print(f"发现 {len(avg_distances)} 个航站对连接")
    
    return all_airports, avg_distances

def build_graph_from_data(airports, distances):
    """
    根据航站和距离数据构建图
    """
    G = nx.Graph()
    
    # 添加所有节点
    for airport in airports:
        G.add_node(airport)
    
    # 添加边和权重
    for (airport1, airport2), distance in distances.items():
        G.add_edge(airport1, airport2, weight=distance)
    
    print(f"图构建完成：{len(G.nodes)} 个节点，{len(G.edges)} 条边")
    return G

# 文件路径
excel_file = "/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/5月航班运行数据（实际数据列）.xlsx"

# 读取数据
airports, distances = load_flight_data(excel_file)

if airports is None or distances is None:
    print("无法读取数据，程序退出")
    exit(1)

# 构建图
G = build_graph_from_data(airports, distances)

# 检查图的连通性
if not nx.is_connected(G):
    print("⚠️  警告：图不连通，将使用最大连通分量")
    # 获取最大连通分量
    largest_cc = max(nx.connected_components(G), key=len)
    G = G.subgraph(largest_cc).copy()
    print(f"使用最大连通分量：{len(G.nodes)} 个节点，{len(G.edges)} 条边")

# 获取初始布局（坐标是归一化的）使用kamada_kawai_layout算法
print("正在计算节点坐标...")
pos = nx.kamada_kawai_layout(G, weight='weight')

# 计算当前布局中的某一条边的实际距离
def euclidean(p1, p2):
    return np.linalg.norm(np.array(p1) - np.array(p2))

# 选择一条边进行缩放参考（选择第一条边）
edges = list(G.edges())
if edges:
    ref_edge = edges[0]
    # 当前坐标中参考边的距离
    d_current = euclidean(pos[ref_edge[0]], pos[ref_edge[1]])
    # 实际目标距离
    d_target = G[ref_edge[0]][ref_edge[1]]['weight']
    
    # 计算缩放因子
    scale_factor = d_target / d_current
    
    # 对所有坐标进行等比缩放
    scaled_pos = {node: scale_factor * np.array(coord) for node, coord in pos.items()}
    
    # 打印还原后的坐标
    print("\n=== 航站坐标 ===")
    for node, coord in scaled_pos.items():
        print(f"航站 {node}: ({coord[0]:.4f}, {coord[1]:.4f})")
    
    # 验证缩放效果（检查参考边的距离）
    scaled_distance = euclidean(scaled_pos[ref_edge[0]], scaled_pos[ref_edge[1]])
    print(f"\n缩放验证：")
    print(f"参考边 {ref_edge[0]}-{ref_edge[1]} 原始距离: {d_target:.2f} 英里")
    print(f"计算后距离: {scaled_distance:.2f} 英里")
    print(f"误差: {abs(scaled_distance - d_target):.4f} 英里")
    
    # 保存坐标到文件
    coords_df = pd.DataFrame([(node, coord[0], coord[1]) for node, coord in scaled_pos.items()], 
                            columns=['航站代码', 'X坐标', 'Y坐标'])
    output_file = "航站坐标.xlsx"
    coords_df.to_excel(output_file, index=False)
    print(f"\n航站坐标已保存到: {output_file}")
    
    # 可视化
    plt.figure(figsize=(15, 10))
    
    # 绘制图
    nx.draw(G, scaled_pos, with_labels=True, node_color='skyblue', 
            node_size=1000, font_size=8, font_weight='bold')
    
    # 添加边的权重标签（只显示部分边以避免过于拥挤）
    edge_labels = nx.get_edge_attributes(G, 'weight')
    # 只显示权重较小的边的标签，避免图过于拥挤
    filtered_edge_labels = {edge: f"{weight:.0f}" for edge, weight in edge_labels.items() if weight < 2000}
    nx.draw_networkx_edge_labels(G, scaled_pos, edge_labels=filtered_edge_labels, font_size=6)
    
    plt.title("航站网络图（基于实际里程数据）", fontsize=14, fontweight='bold')
    plt.axis('equal')
    
    # 保存图片
    plt.savefig('航站网络图.png', dpi=300, bbox_inches='tight')
    print(f"网络图已保存为: 航站网络图.png")
    
    plt.show()
    
    # 输出一些统计信息
    print(f"\n=== 网络统计信息 ===")
    print(f"节点数: {len(G.nodes)}")
    print(f"边数: {len(G.edges)}")
    print(f"网络密度: {nx.density(G):.4f}")
    print(f"平均度: {sum(dict(G.degree()).values()) / len(G.nodes):.2f}")
    
    # 计算一些中心性指标
    print(f"\n=== 重要航站（按度中心性排序）===")
    degree_centrality = nx.degree_centrality(G)
    sorted_airports = sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)
    for i, (airport, centrality) in enumerate(sorted_airports[:10]):
        print(f"{i+1:2d}. {airport}: {centrality:.4f}")

else:
    print("图中没有边，无法进行坐标计算")
    
print("\n程序执行完成！")
