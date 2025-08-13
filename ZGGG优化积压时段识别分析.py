#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZGGG优化积压时段识别分析
重新定义积压时段的识别逻辑，使其更符合实际运营情况
积压时段应该是短时的、波动性的现象，而非长时间持续状态
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 导入仿真器
from ZGGG起飞仿真系统 import ZGGGDepartureSimulator

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

class OptimizedBacklogAnalyzer:
    def __init__(self, delay_threshold=4, base_backlog_threshold=10):
        """
        优化的积压分析器
        
        Args:
            delay_threshold: 延误判定阈值(分钟)
            base_backlog_threshold: 基础积压判定阈值(班次/小时)
        """
        self.delay_threshold = delay_threshold
        self.base_backlog_threshold = base_backlog_threshold
        self.real_data = None
        
        print(f"=== 优化积压分析器初始化 ===")
        print(f"延误判定阈值: {delay_threshold} 分钟")
        print(f"基础积压阈值: {base_backlog_threshold} 班/小时")
    
    def load_real_data(self):
        """载入真实数据"""
        print(f"\n=== 载入真实数据 ===")
        
        df = pd.read_excel('/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/数据/5月航班运行数据（脱敏）.xlsx')
        
        # 提取ZGGG起飞航班
        zggg_dep = df[df['实际起飞站四字码'] == 'ZGGG'].copy()
        
        # 转换时间字段
        time_fields = ['计划离港时间', '实际离港时间', '实际起飞时间']
        for field in time_fields:
            zggg_dep[field] = pd.to_datetime(zggg_dep[field], errors='coerce')
        
        # 只保留有完整时间数据的航班
        self.real_data = zggg_dep.dropna(subset=time_fields).copy()
        
        # 计算延误时间
        self.real_data['起飞延误分钟'] = (
            self.real_data['实际起飞时间'] - self.real_data['计划离港时间']
        ).dt.total_seconds() / 60
        
        print(f"真实数据载入: {len(self.real_data)} 班航班")
        return self.real_data
    
    def analyze_hourly_patterns(self):
        """分析每小时的延误模式"""
        print(f"\n=== 分析每小时延误模式 ===")
        
        # 识别延误航班
        delayed_flights = self.real_data[
            self.real_data['起飞延误分钟'] > self.delay_threshold
        ].copy()
        
        print(f"延误航班总数: {len(delayed_flights)} 班 ({len(delayed_flights)/len(self.real_data)*100:.1f}%)")
        
        # 按小时统计
        delayed_flights['hour'] = delayed_flights['计划离港时间'].dt.hour
        delayed_flights['date'] = delayed_flights['计划离港时间'].dt.date
        
        # 计算每小时的统计信息
        hourly_stats = {}
        
        for hour in range(24):
            hour_data = delayed_flights[delayed_flights['hour'] == hour]
            if len(hour_data) > 0:
                daily_counts = hour_data.groupby('date').size()
                hourly_stats[hour] = {
                    'daily_mean': daily_counts.mean(),
                    'daily_std': daily_counts.std(),
                    'daily_max': daily_counts.max(),
                    'daily_min': daily_counts.min(),
                    'total_days': len(daily_counts),
                    'zero_days': 31 - len(daily_counts)  # 5月31天减去有延误的天数
                }
            else:
                hourly_stats[hour] = {
                    'daily_mean': 0,
                    'daily_std': 0,
                    'daily_max': 0,
                    'daily_min': 0,
                    'total_days': 0,
                    'zero_days': 31
                }
        
        return hourly_stats, delayed_flights
    
    def identify_dynamic_backlog_periods(self, hourly_stats):
        """动态识别积压时段"""
        print(f"\n=== 动态识别积压时段 ===")
        
        # 计算动态阈值
        all_means = [stats['daily_mean'] for stats in hourly_stats.values()]
        overall_mean = np.mean(all_means)
        overall_std = np.std(all_means)
        
        print(f"全天延误航班均值: {overall_mean:.1f} 班/小时")
        print(f"全天延误航班标准差: {overall_std:.1f}")
        
        # 方法1: 基于统计异常的积压识别
        # 积压定义：显著高于平均水平且变异性较大的时段
        backlog_criteria = {
            'statistical': [],  # 统计异常积压
            'absolute': [],     # 绝对阈值积压
            'relative': []      # 相对波动积压
        }
        
        print(f"\n各小时延误情况分析:")
        for hour in range(24):
            stats = hourly_stats[hour]
            mean_val = stats['daily_mean']
            std_val = stats['daily_std']
            max_val = stats['daily_max']
            
            print(f"  {hour:02d}:00-{hour+1:02d}:00: 均值{mean_val:.1f}, 标准差{std_val:.1f}, 最大{max_val}")
            
            # 统计异常标准：均值超过全体均值+1倍标准差
            if mean_val > (overall_mean + overall_std):
                backlog_criteria['statistical'].append(hour)
            
            # 绝对阈值标准：均值超过基础阈值
            if mean_val > self.base_backlog_threshold:
                backlog_criteria['absolute'].append(hour)
            
            # 相对波动标准：标准差较大且均值较高的时段
            if std_val > 5 and mean_val > overall_mean:
                backlog_criteria['relative'].append(hour)
        
        return backlog_criteria, overall_mean, overall_std
    
    def identify_surge_periods(self, hourly_stats):
        """识别流量激增时段"""
        print(f"\n=== 识别流量激增时段 ===")
        
        surge_periods = []
        
        # 载入所有航班数据（不仅仅是延误航班）
        all_flights = self.real_data.copy()
        all_flights['hour'] = all_flights['计划离港时间'].dt.hour
        all_flights['date'] = all_flights['计划离港时间'].dt.date
        
        # 计算每小时总航班量
        hourly_total_stats = {}
        for hour in range(24):
            hour_data = all_flights[all_flights['hour'] == hour]
            daily_counts = hour_data.groupby('date').size()
            hourly_total_stats[hour] = {
                'daily_mean': daily_counts.mean() if len(daily_counts) > 0 else 0,
                'daily_std': daily_counts.std() if len(daily_counts) > 0 else 0
            }
        
        # 识别流量激增导致的积压
        print(f"各小时总航班流量分析:")
        for hour in range(24):
            total_stats = hourly_total_stats[hour]
            delay_stats = hourly_stats[hour]
            
            total_mean = total_stats['daily_mean']
            delay_mean = delay_stats['daily_mean']
            delay_rate = (delay_mean / total_mean * 100) if total_mean > 0 else 0
            
            print(f"  {hour:02d}:00-{hour+1:02d}:00: 总量{total_mean:.1f}, 延误{delay_mean:.1f}, 延误率{delay_rate:.1f}%")
            
            # 积压判定：总量高且延误率高的时段
            if total_mean > 15 and delay_rate > 70:  # 航班量>15且延误率>70%
                surge_periods.append({
                    'hour': hour,
                    'total_flights': total_mean,
                    'delayed_flights': delay_mean,
                    'delay_rate': delay_rate
                })
        
        return surge_periods, hourly_total_stats
    
    def find_continuous_backlog_periods(self, backlog_hours):
        """查找连续积压时段，限制在合理范围内"""
        if not backlog_hours:
            return []
        
        hours = sorted(backlog_hours)
        continuous_periods = []
        current_period = [hours[0]]
        
        for i in range(1, len(hours)):
            if hours[i] - hours[i-1] == 1:  # 连续小时
                current_period.append(hours[i])
            else:
                # 检查连续时段长度
                if 1 <= len(current_period) <= 5:  # 合理的积压持续时间：1-5小时
                    continuous_periods.append(current_period)
                elif len(current_period) > 5:
                    # 长时段可能是持续繁忙而非真正积压，分割处理
                    print(f"  长时段{current_period[0]:02d}:00-{current_period[-1]+1:02d}:00 (持续{len(current_period)}小时) - 可能非真实积压")
                
                current_period = [hours[i]]
        
        # 处理最后一个时段
        if 1 <= len(current_period) <= 5:
            continuous_periods.append(current_period)
        elif len(current_period) > 5:
            print(f"  长时段{current_period[0]:02d}:00-{current_period[-1]+1:02d}:00 (持续{len(current_period)}小时) - 可能非真实积压")
        
        return continuous_periods
    
    def analyze_optimized_backlog(self):
        """综合分析优化后的积压时段"""
        print(f"\n" + "="*60)
        print(f"                优化积压时段分析")
        print(f"="*60)
        
        # 1. 分析每小时模式
        hourly_stats, delayed_flights = self.analyze_hourly_patterns()
        
        # 2. 动态识别积压时段
        backlog_criteria, overall_mean, overall_std = self.identify_dynamic_backlog_periods(hourly_stats)
        
        # 3. 识别流量激增时段
        surge_periods, hourly_total_stats = self.identify_surge_periods(hourly_stats)
        
        # 4. 综合判定最终积压时段
        print(f"\n=== 综合判定积压时段 ===")
        print(f"统计异常积压时段: {len(backlog_criteria['statistical'])} 个: {backlog_criteria['statistical']}")
        print(f"绝对阈值积压时段: {len(backlog_criteria['absolute'])} 个: {backlog_criteria['absolute']}")
        print(f"相对波动积压时段: {len(backlog_criteria['relative'])} 个: {backlog_criteria['relative']}")
        print(f"流量激增积压时段: {len(surge_periods)} 个: {[p['hour'] for p in surge_periods]}")
        
        # 综合多种标准，取交集作为真正的积压时段
        final_backlog_hours = set(backlog_criteria['statistical']) & set([p['hour'] for p in surge_periods])
        
        if not final_backlog_hours:
            # 如果交集为空，使用统计异常标准但限制连续长度
            final_backlog_hours = set(backlog_criteria['statistical'])
        
        print(f"\n最终积压时段: {len(final_backlog_hours)} 个: {sorted(final_backlog_hours)}")
        
        # 5. 查找连续积压时段（限制合理长度）
        continuous_periods = self.find_continuous_backlog_periods(list(final_backlog_hours))
        
        print(f"\n连续积压时段分析:")
        for i, period in enumerate(continuous_periods, 1):
            start, end = period[0], period[-1]
            duration = len(period)
            total_delays = sum([hourly_stats[h]['daily_mean'] for h in period])
            print(f"  连续积压{i}: {start:02d}:00-{end+1:02d}:00 (持续{duration}小时, 日均{total_delays:.1f}班延误)")
        
        return {
            'hourly_stats': hourly_stats,
            'backlog_criteria': backlog_criteria,
            'surge_periods': surge_periods,
            'final_backlog_hours': sorted(final_backlog_hours),
            'continuous_periods': continuous_periods,
            'delayed_flights': delayed_flights,
            'overall_mean': overall_mean,
            'overall_std': overall_std
        }
    
    def visualize_optimized_analysis(self, analysis_result):
        """可视化优化后的分析结果"""
        fig, axes = plt.subplots(3, 2, figsize=(16, 18))
        
        hourly_stats = analysis_result['hourly_stats']
        
        # 1. 每小时延误统计对比
        ax1 = axes[0, 0]
        hours = range(24)
        means = [hourly_stats[h]['daily_mean'] for h in hours]
        stds = [hourly_stats[h]['daily_std'] for h in hours]
        maxs = [hourly_stats[h]['daily_max'] for h in hours]
        
        ax1.bar(hours, means, alpha=0.6, label='日均延误', color='blue')
        ax1.errorbar(hours, means, yerr=stds, fmt='none', color='red', alpha=0.7, label='标准差')
        ax1.plot(hours, maxs, 'ro-', alpha=0.7, label='最大值')
        
        # 标记不同类型的积压时段
        for h in analysis_result['backlog_criteria']['statistical']:
            ax1.axvspan(h-0.4, h+0.4, alpha=0.3, color='red', label='统计异常' if h == analysis_result['backlog_criteria']['statistical'][0] else '')
        
        for period in analysis_result['surge_periods']:
            h = period['hour']
            ax1.axvspan(h-0.4, h+0.4, alpha=0.3, color='orange', label='流量激增' if h == analysis_result['surge_periods'][0]['hour'] else '')
        
        ax1.axhline(y=analysis_result['overall_mean'], color='green', linestyle='--', alpha=0.7, label='全天均值')
        ax1.axhline(y=self.base_backlog_threshold, color='purple', linestyle='--', alpha=0.7, label='基础阈值')
        
        ax1.set_xlabel('小时')
        ax1.set_ylabel('延误航班数')
        ax1.set_title('每小时延误统计分析')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 2. 延误率vs总航班量散点图
        ax2 = axes[0, 1]
        surge_data = analysis_result['surge_periods']
        if surge_data:
            total_flights = [p['total_flights'] for p in surge_data]
            delay_rates = [p['delay_rate'] for p in surge_data]
            hours_surge = [p['hour'] for p in surge_data]
            
            scatter = ax2.scatter(total_flights, delay_rates, c=hours_surge, cmap='viridis', s=100, alpha=0.7)
            plt.colorbar(scatter, ax=ax2, label='小时')
            
            for i, (x, y, h) in enumerate(zip(total_flights, delay_rates, hours_surge)):
                ax2.annotate(f'{h:02d}h', (x, y), xytext=(5, 5), textcoords='offset points', fontsize=10)
        
        ax2.set_xlabel('总航班量(架/小时)')
        ax2.set_ylabel('延误率(%)')
        ax2.set_title('流量vs延误率分析')
        ax2.grid(True, alpha=0.3)
        
        # 3. 积压时段对比（不同标准）
        ax3 = axes[1, 0]
        criteria_names = ['统计异常', '绝对阈值', '相对波动', '流量激增', '最终结果']
        criteria_counts = [
            len(analysis_result['backlog_criteria']['statistical']),
            len(analysis_result['backlog_criteria']['absolute']),
            len(analysis_result['backlog_criteria']['relative']),
            len(analysis_result['surge_periods']),
            len(analysis_result['final_backlog_hours'])
        ]
        
        bars = ax3.bar(criteria_names, criteria_counts, alpha=0.7, color=['red', 'orange', 'yellow', 'green', 'blue'])
        ax3.set_ylabel('积压时段数量')
        ax3.set_title('不同标准下的积压时段识别')
        plt.setp(ax3.get_xticklabels(), rotation=45, ha='right')
        
        # 添加数值标签
        for bar, count in zip(bars, criteria_counts):
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{count}', ha='center', va='bottom', fontweight='bold')
        
        # 4. 连续积压时段可视化
        ax4 = axes[1, 1]
        continuous_periods = analysis_result['continuous_periods']
        if continuous_periods:
            colors = plt.cm.Set3(np.linspace(0, 1, len(continuous_periods)))
            for i, (period, color) in enumerate(zip(continuous_periods, colors)):
                start, end = period[0], period[-1]
                duration = len(period)
                ax4.barh(i, duration, left=start, alpha=0.7, color=color)
                ax4.text(start + duration/2, i, f'{duration}h', ha='center', va='center', fontweight='bold')
            
            ax4.set_xlabel('小时')
            ax4.set_ylabel('连续积压时段序号')
            ax4.set_title('连续积压时段分析')
            ax4.set_xlim(0, 24)
            ax4.set_xticks(range(0, 25, 4))
            ax4.set_xticklabels([f'{i}:00' for i in range(0, 25, 4)])
        else:
            ax4.text(0.5, 0.5, '无连续积压时段', ha='center', va='center', transform=ax4.transAxes, fontsize=14)
            ax4.set_title('连续积压时段分析')
        
        # 5. 优化前后对比
        ax5 = axes[2, 0]
        old_method_hours = list(range(7, 23))  # 原来的方法：7-22点全部认为是积压
        new_method_hours = analysis_result['final_backlog_hours']
        
        comparison_data = {
            '原方法': len(old_method_hours),
            '优化方法': len(new_method_hours),
            '差异': len(old_method_hours) - len(new_method_hours)
        }
        
        bars = ax5.bar(comparison_data.keys(), comparison_data.values(), 
                       color=['red', 'green', 'orange'], alpha=0.7)
        ax5.set_ylabel('积压时段数量')
        ax5.set_title('优化前后积压识别对比')
        
        for bar, (key, value) in zip(bars, comparison_data.items()):
            height = bar.get_height()
            ax5.text(bar.get_x() + bar.get_width()/2., height + (0.1 if height >= 0 else -0.3),
                    f'{value}', ha='center', va='bottom' if height >= 0 else 'top', fontweight='bold')
        
        # 6. 积压强度分析
        ax6 = axes[2, 1]
        final_hours = analysis_result['final_backlog_hours']
        if final_hours:
            intensities = [hourly_stats[h]['daily_mean'] for h in final_hours]
            ax6.bar(range(len(final_hours)), intensities, alpha=0.7, color='red')
            ax6.set_xticks(range(len(final_hours)))
            ax6.set_xticklabels([f'{h:02d}h' for h in final_hours])
            ax6.set_ylabel('平均延误航班数')
            ax6.set_title('积压时段强度分析')
        else:
            ax6.text(0.5, 0.5, '无积压时段', ha='center', va='center', transform=ax6.transAxes, fontsize=14)
            ax6.set_title('积压时段强度分析')
        
        plt.tight_layout()
        plt.savefig('ZGGG优化积压时段分析.png', dpi=300, bbox_inches='tight')
        plt.show()
        
        return fig

def main():
    """主函数"""
    print("=== ZGGG优化积压时段识别分析 ===")
    
    # 初始化分析器
    analyzer = OptimizedBacklogAnalyzer(delay_threshold=4, base_backlog_threshold=10)
    
    # 载入数据
    analyzer.load_real_data()
    
    # 执行优化分析
    analysis_result = analyzer.analyze_optimized_backlog()
    
    # 可视化结果
    analyzer.visualize_optimized_analysis(analysis_result)
    
    # 输出总结
    print(f"\n" + "="*60)
    print(f"                积压分析优化总结")
    print(f"="*60)
    print(f"✅ 原方法识别积压时段: 16个 (7:00-22:00连续)")
    print(f"✅ 优化方法识别积压时段: {len(analysis_result['final_backlog_hours'])}个: {analysis_result['final_backlog_hours']}")
    print(f"✅ 连续积压时段数: {len(analysis_result['continuous_periods'])}个")
    
    if analysis_result['continuous_periods']:
        for i, period in enumerate(analysis_result['continuous_periods'], 1):
            start, end = period[0], period[-1]
            duration = len(period)
            print(f"   连续积压{i}: {start:02d}:00-{end+1:02d}:00 (持续{duration}小时)")
    
    print(f"\n🎯 优化策略:")
    print(f"   • 采用统计异常检测替代固定阈值")
    print(f"   • 结合流量激增分析")
    print(f"   • 限制连续积压时长在1-5小时范围")
    print(f"   • 区分持续繁忙与真正积压")
    
    return analyzer, analysis_result

if __name__ == "__main__":
    analyzer, analysis_result = main()
