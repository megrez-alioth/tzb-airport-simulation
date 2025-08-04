import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from scipy import stats
from sklearn.preprocessing import LabelEncoder
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

def load_and_explore_data(file_path):
    """
    加载数据并进行初步探索
    """
    print("=== 数据加载与探索 ===")
    
    try:
        df = pd.read_excel(file_path)
        print(f"数据形状: {df.shape}")
        print(f"\n列名: {list(df.columns)}")
        print(f"\n前5行数据预览:")
        print(df.head())
        print(f"\n数据类型:")
        print(df.dtypes)
        print(f"\n缺失值统计:")
        print(df.isnull().sum())
        
        return df
    except Exception as e:
        print(f"数据加载失败: {e}")
        return None

def calculate_flight_speed(df):
    """
    计算飞行速度
    """
    print("\n=== 计算飞行速度 ===")
    
    # 复制数据
    df_speed = df.copy()
    
    # 检查必要的列是否存在
    required_columns = ['实际起飞时间', '实际落地时间', '机尾号', '机型']
    missing_columns = [col for col in required_columns if col not in df_speed.columns]
    
    # 检查航程列（可能有不同的名称）
    distance_col = None
    possible_distance_cols = ['实际航程', '实际航程_Mile', '航程', '距离']
    for col in possible_distance_cols:
        if col in df_speed.columns:
            distance_col = col
            break
    
    if distance_col is None:
        missing_columns.append('实际航程(或相关列)')
    
    if missing_columns:
        print(f"缺少必要列: {missing_columns}")
        # 尝试查找相似的列名
        print("可用列名:")
        for col in df_speed.columns:
            print(f"  - {col}")
        return None
    
    print(f"使用航程列: {distance_col}")
    
    # 将英里转换为公里（如果需要）
    if 'Mile' in distance_col:
        df_speed['实际航程_km'] = df_speed[distance_col] * 1.60934  # 1英里 = 1.60934公里
        print("已将英里转换为公里")
    else:
        df_speed['实际航程_km'] = df_speed[distance_col]
    
    # 计算飞行时间（小时）
    def parse_time_to_datetime(time_str):
        """解析时间字符串为datetime对象"""
        if pd.isna(time_str):
            return None
        
        try:
            if isinstance(time_str, str):
                # 尝试多种时间格式
                formats = [
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M",
                    "%Y/%m/%d %H:%M:%S", 
                    "%Y/%m/%d %H:%M",
                    "%m/%d/%Y %H:%M:%S",
                    "%m/%d/%Y %H:%M"
                ]
                
                for fmt in formats:
                    try:
                        return datetime.strptime(time_str, fmt)
                    except ValueError:
                        continue
                        
            elif hasattr(time_str, 'to_pydatetime'):
                return time_str.to_pydatetime()
            elif isinstance(time_str, datetime):
                return time_str
                
            return None
        except:
            return None
    
    # 解析时间
    df_speed['起飞时间_dt'] = df_speed['实际起飞时间'].apply(parse_time_to_datetime)
    df_speed['落地时间_dt'] = df_speed['实际落地时间'].apply(parse_time_to_datetime)
    
    # 过滤有效数据
    valid_mask = (
        df_speed['起飞时间_dt'].notna() & 
        df_speed['落地时间_dt'].notna() & 
        df_speed['实际航程_km'].notna() & 
        df_speed['机尾号'].notna() & 
        df_speed['机型'].notna()
    )
    
    df_speed = df_speed[valid_mask].copy()
    print(f"有效数据量: {len(df_speed)}")
    
    if len(df_speed) == 0:
        print("没有有效数据进行分析")
        return None
    
    # 计算飞行时间（小时）
    df_speed['飞行时间_小时'] = (
        df_speed['落地时间_dt'] - df_speed['起飞时间_dt']
    ).dt.total_seconds() / 3600
    
    # 过滤合理的飞行时间（0.5小时到20小时）
    time_mask = (df_speed['飞行时间_小时'] > 0.5) & (df_speed['飞行时间_小时'] < 20)
    df_speed = df_speed[time_mask].copy()
    
    # 过滤合理的航程（大于0）
    distance_mask = df_speed['实际航程_km'] > 0
    df_speed = df_speed[distance_mask].copy()
    
    # 计算平均速度（公里/小时）
    df_speed['平均速度_kmh'] = df_speed['实际航程_km'] / df_speed['飞行时间_小时']
    
    # 过滤合理的速度范围（200-1000 km/h，适合民航飞机）
    speed_mask = (df_speed['平均速度_kmh'] >= 200) & (df_speed['平均速度_kmh'] <= 1000)
    df_speed = df_speed[speed_mask].copy()
    
    print(f"过滤后有效数据量: {len(df_speed)}")
    print(f"平均速度统计:")
    print(df_speed['平均速度_kmh'].describe())
    
    return df_speed

def analyze_aircraft_type_correlation(df):
    """
    分析机型与飞行速度的相关性
    """
    print("\n=== 机型与飞行速度相关性分析 ===")
    
    # 按机型分组统计
    aircraft_stats = df.groupby('机型')['平均速度_kmh'].agg([
        'count', 'mean', 'std', 'min', 'max'
    ]).round(2)
    
    aircraft_stats.columns = ['航班数量', '平均速度', '速度标准差', '最低速度', '最高速度']
    aircraft_stats = aircraft_stats[aircraft_stats['航班数量'] >= 5]  # 只保留样本量≥5的机型
    
    print("各机型飞行速度统计:")
    print(aircraft_stats.sort_values('平均速度', ascending=False))
    
    # 计算机型对速度的解释度
    if len(aircraft_stats) >= 2:
        # 使用方差分析计算机型间差异
        aircraft_groups = []
        aircraft_names = []
        
        for aircraft_type in aircraft_stats.index:
            speeds = df[df['机型'] == aircraft_type]['平均速度_kmh'].values
            if len(speeds) >= 5:
                aircraft_groups.append(speeds)
                aircraft_names.append(aircraft_type)
        
        if len(aircraft_groups) >= 2:
            # 进行方差分析
            f_stat, p_value = stats.f_oneway(*aircraft_groups)
            print(f"\n机型间速度差异方差分析:")
            print(f"F统计量: {f_stat:.4f}")
            print(f"P值: {p_value:.4f}")
            
            # 计算组间方差占总方差的比例（η²）
            total_var = df['平均速度_kmh'].var()
            group_means = [np.mean(group) for group in aircraft_groups]
            group_sizes = [len(group) for group in aircraft_groups]
            overall_mean = df['平均速度_kmh'].mean()
            
            between_group_var = sum(size * (mean - overall_mean)**2 for size, mean in zip(group_sizes, group_means)) / (len(df) - 1)
            eta_squared = between_group_var / total_var
            
            print(f"机型解释的速度方差比例 (η²): {eta_squared:.4f} ({eta_squared*100:.2f}%)")
            
            return eta_squared, aircraft_stats
    
    return 0, aircraft_stats

def analyze_route_correlation(df):
    """
    分析航线与飞行速度的相关性
    """
    print("\n=== 航线与飞行速度相关性分析 ===")
    
    # 创建航线标识
    df['航线'] = df['计划起飞站四字码'] + '-' + df['计划到达站四字码']
    
    # 按航线分组统计
    route_stats = df.groupby('航线')['平均速度_kmh'].agg([
        'count', 'mean', 'std', 'min', 'max'
    ]).round(2)
    
    route_stats.columns = ['航班数量', '平均速度', '速度标准差', '最低速度', '最高速度']
    route_stats = route_stats[route_stats['航班数量'] >= 5]  # 只保留样本量≥5的航线
    
    print(f"分析的航线数量: {len(route_stats)}")
    print("航线飞行速度统计（前10条）:")
    print(route_stats.sort_values('平均速度', ascending=False).head(10))
    
    # 计算航线对速度的解释度
    if len(route_stats) >= 2:
        # 使用方差分析计算航线间差异
        route_groups = []
        route_names = []
        
        for route in route_stats.index:
            speeds = df[df['航线'] == route]['平均速度_kmh'].values
            if len(speeds) >= 5:
                route_groups.append(speeds)
                route_names.append(route)
        
        if len(route_groups) >= 2:
            # 进行方差分析
            f_stat, p_value = stats.f_oneway(*route_groups)
            print(f"\n航线间速度差异方差分析:")
            print(f"F统计量: {f_stat:.4f}")
            print(f"P值: {p_value:.4f}")
            
            # 计算组间方差占总方差的比例（η²）
            total_var = df['平均速度_kmh'].var()
            group_means = [np.mean(group) for group in route_groups]
            group_sizes = [len(group) for group in route_groups]
            overall_mean = df['平均速度_kmh'].mean()
            
            between_group_var = sum(size * (mean - overall_mean)**2 for size, mean in zip(group_sizes, group_means)) / (len(df) - 1)
            eta_squared = between_group_var / total_var
            
            print(f"航线解释的速度方差比例 (η²): {eta_squared:.4f} ({eta_squared*100:.2f}%)")
            
            return eta_squared, route_stats
    
    return 0, route_stats

def compare_correlations(aircraft_eta, route_eta):
    """
    比较机型和航线的相关性
    """
    print("\n=== 相关性比较结果 ===")
    
    print(f"机型对飞行速度的解释度: {aircraft_eta:.4f} ({aircraft_eta*100:.2f}%)")
    print(f"航线对飞行速度的解释度: {route_eta:.4f} ({route_eta*100:.2f}%)")
    
    if aircraft_eta > route_eta:
        diff = aircraft_eta - route_eta
        print(f"\n结论: 机型对飞行速度的影响更大")
        print(f"机型的解释度比航线高 {diff:.4f} ({diff*100:.2f}个百分点)")
    elif route_eta > aircraft_eta:
        diff = route_eta - aircraft_eta
        print(f"\n结论: 航线对飞行速度的影响更大")
        print(f"航线的解释度比机型高 {diff:.4f} ({diff*100:.2f}个百分点)")
    else:
        print(f"\n结论: 机型和航线对飞行速度的影响相当")
    
    # 计算相对差异
    if max(aircraft_eta, route_eta) > 0:
        relative_diff = abs(aircraft_eta - route_eta) / max(aircraft_eta, route_eta)
        print(f"相对差异: {relative_diff:.2%}")

def create_visualizations(df):
    """
    创建可视化图表
    """
    print("\n=== 生成可视化图表 ===")
    
    # 创建子图
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('飞行速度分析', fontsize=16, fontweight='bold')
    
    # 1. 机型速度分布箱线图
    aircraft_counts = df['机型'].value_counts()
    top_aircraft = aircraft_counts[aircraft_counts >= 10].index[:8]  # 选取前8个机型
    
    if len(top_aircraft) > 0:
        ax1 = axes[0, 0]
        df_top_aircraft = df[df['机型'].isin(top_aircraft)]
        
        box_data = [df_top_aircraft[df_top_aircraft['机型'] == aircraft]['平均速度_kmh'].values 
                   for aircraft in top_aircraft]
        
        ax1.boxplot(box_data, labels=top_aircraft)
        ax1.set_title('主要机型飞行速度分布')
        ax1.set_xlabel('机型')
        ax1.set_ylabel('平均速度 (km/h)')
        ax1.tick_params(axis='x', rotation=45)
    
    # 2. 航线速度分布（热门航线）
    route_counts = df['航线'].value_counts()
    top_routes = route_counts[route_counts >= 10].index[:10]
    
    if len(top_routes) > 0:
        ax2 = axes[0, 1]
        df_top_routes = df[df['航线'].isin(top_routes)]
        
        box_data = [df_top_routes[df_top_routes['航线'] == route]['平均速度_kmh'].values 
                   for route in top_routes]
        
        ax2.boxplot(box_data, labels=top_routes)
        ax2.set_title('热门航线飞行速度分布')
        ax2.set_xlabel('航线')
        ax2.set_ylabel('平均速度 (km/h)')
        ax2.tick_params(axis='x', rotation=45)
    
    # 3. 速度分布直方图
    ax3 = axes[1, 0]
    ax3.hist(df['平均速度_kmh'], bins=30, alpha=0.7, edgecolor='black')
    ax3.set_title('飞行速度分布直方图')
    ax3.set_xlabel('平均速度 (km/h)')
    ax3.set_ylabel('频次')
    ax3.axvline(df['平均速度_kmh'].mean(), color='red', linestyle='--', 
                label=f'平均值: {df["平均速度_kmh"].mean():.1f} km/h')
    ax3.legend()
    
    # 4. 航程与速度关系散点图
    ax4 = axes[1, 1]
    scatter = ax4.scatter(df['实际航程_km'], df['平均速度_kmh'], alpha=0.6, s=20)
    ax4.set_title('航程与飞行速度关系')
    ax4.set_xlabel('实际航程 (km)')
    ax4.set_ylabel('平均速度 (km/h)')
    
    # 添加趋势线
    z = np.polyfit(df['实际航程_km'], df['平均速度_kmh'], 1)
    p = np.poly1d(z)
    ax4.plot(df['实际航程_km'], p(df['实际航程_km']), "r--", alpha=0.8)
    
    # 计算相关系数
    correlation = df['实际航程_km'].corr(df['平均速度_kmh'])
    ax4.text(0.05, 0.95, f'相关系数: r = {correlation:.3f}', 
             transform=ax4.transAxes, bbox=dict(boxstyle="round", facecolor='wheat'))
    
    plt.tight_layout()
    plt.savefig('飞行速度分析.png', dpi=300, bbox_inches='tight')
    print("图表已保存为: 飞行速度分析.png")
    
    return fig

def main():
    """
    主函数
    """
    # 数据文件路径
    file_path = "/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/三段式飞行- plan所需数据.xlsx"
    
    # 1. 加载和探索数据
    df = load_and_explore_data(file_path)
    if df is None:
        return
    
    # 2. 计算飞行速度
    df_speed = calculate_flight_speed(df)
    if df_speed is None:
        return
    
    # 3. 分析机型相关性
    aircraft_eta, aircraft_stats = analyze_aircraft_type_correlation(df_speed)
    
    # 4. 分析航线相关性  
    route_eta, route_stats = analyze_route_correlation(df_speed)
    
    # 5. 比较结果
    compare_correlations(aircraft_eta, route_eta)
    
    # 6. 生成可视化
    try:
        create_visualizations(df_speed)
    except Exception as e:
        print(f"可视化生成失败: {e}")
    
    # 7. 生成详细报告
    print("\n=== 详细分析报告 ===")
    print(f"总样本量: {len(df_speed)}")
    print(f"分析的机型数量: {df_speed['机型'].nunique()}")
    print(f"分析的航线数量: {df_speed['航线'].nunique()}")
    print(f"分析的飞机数量: {df_speed['机尾号'].nunique()}")
    
    print("\n速度统计信息:")
    print(f"  平均速度: {df_speed['平均速度_kmh'].mean():.2f} km/h")
    print(f"  速度标准差: {df_speed['平均速度_kmh'].std():.2f} km/h")
    print(f"  速度范围: {df_speed['平均速度_kmh'].min():.2f} - {df_speed['平均速度_kmh'].max():.2f} km/h")

if __name__ == "__main__":
    main()
