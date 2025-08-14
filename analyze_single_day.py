import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator

def analyze_single_day(simulator, target_date):
    """分析单日的航班延误和积压情况"""
    
    # 检查数据是否已加载和仿真已完成
    if len(simulator.all_simulation_results) == 0:
        print("错误: 需要先完成仿真")
        return None
    
    # 获取目标日期的仿真数据
    sim_data = simulator.all_simulation_results[
        simulator.all_simulation_results['计划起飞'].dt.date == target_date
    ].copy()
    
    # 获取目标日期的实际数据
    real_data = simulator.data[
        simulator.data['计划离港时间'].dt.date == target_date
    ].copy()
    
    if len(sim_data) == 0 or len(real_data) == 0:
        print(f"错误: 未找到{target_date}的航班数据")
        return None
        
    print(f"当日航班数: {len(sim_data)} 班")
    
    # 基本统计
    print("\n--- 单日延误统计分析 ---")
    sim_delay_mean = sim_data['仿真延误分钟'].mean()
    real_delay_mean = real_data['起飞延误分钟'].mean()
    
    print(f"仿真平均延误: {sim_delay_mean:.1f} 分钟")
    print(f"实际平均延误: {real_delay_mean:.1f} 分钟")
    print(f"延误差异: {abs(sim_delay_mean - real_delay_mean):.1f} 分钟")
    
    # 延误航班数量
    sim_delayed = sim_data[sim_data['仿真延误分钟'] > simulator.delay_threshold]
    real_delayed = real_data[real_data['起飞延误分钟'] > simulator.delay_threshold]
    
    print(f"仿真延误航班数(>{simulator.delay_threshold}分钟): {len(sim_delayed)} 班 ({len(sim_delayed)/len(sim_data)*100:.1f}%)")
    print(f"实际延误航班数(>{simulator.delay_threshold}分钟): {len(real_delayed)} 班 ({len(real_delayed)/len(real_data)*100:.1f}%)")
    
    # 按小时统计数据
    sim_data['小时'] = sim_data['计划起飞'].dt.hour
    real_data['小时'] = real_data['计划离港时间'].dt.hour
    
    # 延误航班数和平均延误时间的小时分布
    hourly_stats = pd.DataFrame(index=range(24))
    
    # 仿真延误航班数
    sim_hourly_counts = sim_data.groupby('小时').size()
    sim_hourly_delayed = sim_data[sim_data['仿真延误分钟'] > simulator.delay_threshold].groupby('小时').size()
    hourly_stats['仿真总航班'] = sim_hourly_counts
    hourly_stats['仿真延误航班'] = sim_hourly_delayed.reindex(range(24), fill_value=0)
    
    # 实际延误航班数
    real_hourly_counts = real_data.groupby('小时').size()
    real_hourly_delayed = real_data[real_data['起飞延误分钟'] > simulator.delay_threshold].groupby('小时').size()
    hourly_stats['实际总航班'] = real_hourly_counts
    hourly_stats['实际延误航班'] = real_hourly_delayed.reindex(range(24), fill_value=0)
    
    # 计算平均延误时间
    sim_hourly_delay = sim_data.groupby('小时')['仿真延误分钟'].mean()
    real_hourly_delay = real_data.groupby('小时')['起飞延误分钟'].mean()
    
    hourly_stats['仿真平均延误'] = sim_hourly_delay.reindex(range(24), fill_value=0)
    hourly_stats['实际平均延误'] = real_hourly_delay.reindex(range(24), fill_value=0)
    
    # 填充缺失值
    hourly_stats = hourly_stats.fillna(0)
    
    return {
        'sim_data': sim_data,
        'real_data': real_data,
        'hourly_stats': hourly_stats,
        'sim_delayed': sim_delayed,
        'real_delayed': real_delayed,
        'target_date': target_date
    }

def visualize_single_day_analysis(simulator, target_date=None):
    """分析和可视化单日的航班延误和积压情况"""
    if not target_date:
        # 允许用户选择日期
        print("\n=== 单日航班延误和积压分析 ===")
        try:
            date_str = input("请输入要分析的日期 (YYYY-MM-DD，默认2025-05-15): ").strip()
            if not date_str:
                target_date = pd.to_datetime('2025-05-15').date()
            else:
                target_date = pd.to_datetime(date_str).date()
        except:
            print("日期格式无效，使用默认日期2025-05-15")
            target_date = pd.to_datetime('2025-05-15').date()
    
    # 分析数据
    results = analyze_single_day(simulator, target_date)
    
    if not results:
        return
        
    # 获取分析结果
    sim_data = results['sim_data']
    real_data = results['real_data']
    hourly_stats = results['hourly_stats']
    sim_delayed = results['sim_delayed']
    real_delayed = results['real_delayed']
    
    # 创建可视化图表
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle(f'ZGGG机场单日航班延误和积压分析 ({target_date})', fontsize=16)
    
    # 1. 按小时统计航班延误数量
    hours = range(24)
    x = np.arange(len(hours))
    width = 0.35
    
    axes[0, 0].bar(x - width/2, hourly_stats['实际延误航班'], width, label='实际延误航班', color='orange', alpha=0.7)
    axes[0, 0].bar(x + width/2, hourly_stats['仿真延误航班'], width, label='仿真延误航班', color='skyblue', alpha=0.7)
    axes[0, 0].axhline(y=simulator.backlog_threshold, color='red', linestyle='--', 
                     label=f'积压阈值({simulator.backlog_threshold}班)')
    
    axes[0, 0].set_title('各小时延误航班数')
    axes[0, 0].set_xlabel('小时')
    axes[0, 0].set_ylabel('延误航班数')
    axes[0, 0].set_xticks(x)
    axes[0, 0].set_xticklabels(hours)
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)
    
    # 标记积压小时段
    backlog_hours_real = [h for h in hours if h in hourly_stats.index and hourly_stats.loc[h, '实际延误航班'] >= simulator.backlog_threshold]
    backlog_hours_sim = [h for h in hours if h in hourly_stats.index and hourly_stats.loc[h, '仿真延误航班'] >= simulator.backlog_threshold]
    
    if backlog_hours_real:
        print(f"\n实际积压时段: {backlog_hours_real}")
    if backlog_hours_sim:
        print(f"仿真积压时段: {backlog_hours_sim}")
    
    for hour in backlog_hours_real:
        axes[0, 0].axvspan(hour-0.5, hour+0.5, alpha=0.2, color='red')
    
    for hour in backlog_hours_sim:
        axes[0, 0].axvspan(hour-0.5, hour+0.5, alpha=0.2, color='blue')
    
    # 2. 平均延误时间
    axes[0, 1].bar(x - width/2, hourly_stats['实际平均延误'], width, label='实际平均延误', color='orange', alpha=0.7)
    axes[0, 1].bar(x + width/2, hourly_stats['仿真平均延误'], width, label='仿真平均延误', color='skyblue', alpha=0.7)
    axes[0, 1].axhline(y=simulator.delay_threshold, color='red', linestyle='--', 
                     label=f'延误阈值({simulator.delay_threshold}分钟)')
    
    axes[0, 1].set_title('各小时平均延误时间')
    axes[0, 1].set_xlabel('小时')
    axes[0, 1].set_ylabel('平均延误(分钟)')
    axes[0, 1].set_xticks(x)
    axes[0, 1].set_xticklabels(hours)
    axes[0, 1].legend()
    axes[0, 1].grid(True, alpha=0.3)
    
    # 3. 延误分布散点图
    sim_data_sorted = sim_data.sort_values('计划起飞')
    real_data_sorted = real_data.sort_values('计划离港时间')
    
    # 添加序号便于比较
    sim_data_sorted['序号'] = range(len(sim_data_sorted))
    real_data_sorted['序号'] = range(len(real_data_sorted))
    
    # 绘制散点图
    axes[1, 0].scatter(sim_data_sorted['序号'], sim_data_sorted['仿真延误分钟'], 
                      alpha=0.7, color='blue', label='仿真延误')
    axes[1, 0].scatter(real_data_sorted['序号'], real_data_sorted['起飞延误分钟'], 
                      alpha=0.7, color='red', label='实际延误')
    
    axes[1, 0].axhline(y=simulator.delay_threshold, color='red', linestyle='--', 
                     label=f'延误阈值({simulator.delay_threshold}分钟)')
    
    axes[1, 0].set_title('航班延误散点图')
    axes[1, 0].set_xlabel('航班序号(按计划起飞时间排序)')
    axes[1, 0].set_ylabel('延误时间(分钟)')
    axes[1, 0].legend()
    axes[1, 0].grid(True, alpha=0.3)
    
    # 4. 延误直方图比较
    max_delay = max(sim_data['仿真延误分钟'].max(), real_data['起飞延误分钟'].max())
    bin_width = 10  # 10分钟一个柱
    bins = range(0, int(max_delay) + bin_width, bin_width)
    
    axes[1, 1].hist(real_data['起飞延误分钟'], bins=bins, alpha=0.5, label='实际延误', color='orange')
    axes[1, 1].hist(sim_data['仿真延误分钟'], bins=bins, alpha=0.5, label='仿真延误', color='skyblue')
    
    axes[1, 1].axvline(x=simulator.delay_threshold, color='red', linestyle='--', 
                     label=f'延误阈值({simulator.delay_threshold}分钟)')
    
    axes[1, 1].set_title('延误时间分布直方图')
    axes[1, 1].set_xlabel('延误时间(分钟)')
    axes[1, 1].set_ylabel('航班数量')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3)
    
    # 保存和显示图表
    plt.tight_layout(rect=[0, 0, 1, 0.96])  # 调整布局，为标题留出空间
    
    filename = f'ZGGG单日航班分析_{target_date}.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"单日分析图表已保存为: {filename}")
    
    # 添加详细数据分析文本输出
    print("\n--- 单日积压时段分析 ---")
    
    # 检查是否存在积压
    if not backlog_hours_real and not backlog_hours_sim:
        print("当日无积压时段")
    else:
        print("积压时段分析:")
        
        # 找出连续的积压时段
        def find_continuous_periods(hours_list):
            if not hours_list:
                return []
            
            hours_list = sorted(hours_list)
            periods = []
            start = hours_list[0]
            end = hours_list[0]
            
            for i in range(1, len(hours_list)):
                if hours_list[i] == end + 1:
                    end = hours_list[i]
                else:
                    periods.append((start, end))
                    start = hours_list[i]
                    end = hours_list[i]
            
            periods.append((start, end))
            return periods
        
        real_periods = find_continuous_periods(backlog_hours_real)
        sim_periods = find_continuous_periods(backlog_hours_sim)
        
        print(f"实际连续积压区间: {real_periods}")
        print(f"仿真连续积压区间: {sim_periods}")
        
        # 对比分析
        if real_periods and sim_periods:
            print("\n积压区间对比:")
            for i, real_period in enumerate(real_periods):
                if i < len(sim_periods):
                    sim_period = sim_periods[i]
                    start_error = abs(sim_period[0] - real_period[0])
                    end_error = abs(sim_period[1] - real_period[1])
                    
                    print(f"区间{i+1}: 实际({real_period[0]}-{real_period[1]})，仿真({sim_period[0]}-{sim_period[1]})")
                    print(f"  起始误差: {start_error}小时，结束误差: {end_error}小时")
    
    return filename
