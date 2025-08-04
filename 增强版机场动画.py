import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.widgets import Slider, Button
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from 机场排队仿真系统 import AirportQueueSimulator
import matplotlib.patches as patches
from collections import defaultdict
import math

# 设置中文字体支持
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'PingFang SC', 'SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# 检查并设置可用的中文字体
import matplotlib.font_manager as fm
available_fonts = [f.name for f in fm.fontManager.ttflist]
chinese_fonts = ['Arial Unicode MS', 'PingFang SC', 'Hiragino Sans GB', 'STHeiti', 'SimHei', 'Microsoft YaHei']
for font in chinese_fonts:
    if font in available_fonts:
        plt.rcParams['font.sans-serif'] = [font] + plt.rcParams['font.sans-serif']
        print(f"使用中文字体: {font}")
        break
else:
    print("⚠️ 未找到合适的中文字体，可能显示为方框")

class EnhancedAirportQueueAnimation:
    def __init__(self, simulation_results, flight_plans, start_time=0, end_time=1440):
        """
        增强版机场排队动画
        """
        self.simulation_results = simulation_results
        self.flight_plans = flight_plans
        self.start_time = start_time
        self.end_time = end_time
        self.current_time = start_time
        
        # 分析数据 - 选择流量最大的前9个机场
        self.airports = self.select_busiest_airports(simulation_results, max_airports=9)
        
        # 创建事件时间线
        self.events = self.create_timeline_events()
        
        # 设置显示参数
        self.airport_width = 8   # 减小宽度以适应多列布局
        self.airport_height = 2.5  # 减小高度
        self.runway_length = 6   # 缩短跑道长度
        self.runway_width = 0.2  # 缩小跑道宽度
        
        # 计算网格布局 - 最多9个机场，3x3排列
        num_airports = len(self.airports)
        if num_airports <= 4:
            self.cols = 2
            self.rows = 2
        elif num_airports <= 9:
            self.cols = 3
            self.rows = 3
        else:
            self.cols = 3
            self.rows = 3
        
        # 动画控制
        self.is_playing = False
        self.animation_speed = 1  # 分钟/帧
        
        # 初始化图形
        self.setup_enhanced_figure()
    
    def select_busiest_airports(self, simulation_results, max_airports=9):
        """选择流量最大的前N个机场"""
        from collections import Counter
        
        # 统计每个机场的活动次数
        airport_counts = Counter()
        for result in simulation_results:
            airport_counts[result['airport']] += 1
        
        # 选择前N个最繁忙的机场
        busiest_airports = [airport for airport, count in airport_counts.most_common(max_airports)]
        
        print(f"=== 选择流量最大的前{max_airports}个机场 ===")
        for i, (airport, count) in enumerate(airport_counts.most_common(max_airports), 1):
            print(f"{i:2d}. {airport}: {count} 次活动")
        
        if len(busiest_airports) < len(airport_counts):
            print(f"共有 {len(airport_counts)} 个机场，已筛选显示前 {len(busiest_airports)} 个")
        
        return busiest_airports
        
    def create_timeline_events(self):
        """创建详细的事件时间线"""
        events = []
        
        for result in self.simulation_results:
            # 只处理选中的机场
            if result['airport'] not in self.airports:
                continue
                
            start_time = self.parse_time_string(result['actual_start'])
            end_time = self.parse_time_string(result['actual_end'])
            
            # 开始事件
            events.append({
                'time': start_time,
                'type': 'start',
                'activity_type': result['activity_type'],
                'aircraft_id': result['aircraft_id'],
                'airport': result['airport'],
                'runway': result.get('runway', 1),
                'end_time': end_time,
                'delay': result['delay_minutes'],
                'queue_position': result.get('queue_position', 1)
            })
            
            # 结束事件
            events.append({
                'time': end_time,
                'type': 'end',
                'activity_type': result['activity_type'],
                'aircraft_id': result['aircraft_id'],
                'airport': result['airport'],
                'runway': result.get('runway', 1)
            })
        
        events.sort(key=lambda x: x['time'])
        return events
    
    def parse_time_string(self, time_str):
        """解析时间字符串"""
        if isinstance(time_str, str):
            parts = time_str.split(':')
            hours = int(parts[0])
            minutes = int(parts[1])
            return hours * 60 + minutes
        return 0
    
    def setup_enhanced_figure(self):
        """设置增强版图形界面"""
        # 根据网格布局调整窗口大小
        fig_width = self.cols * (self.airport_width + 1) + 2
        fig_height = self.rows * (self.airport_height + 1) + 6
        self.fig = plt.figure(figsize=(fig_width, fig_height))
        
        # 主显示区域
        self.ax = plt.subplot2grid((12, 1), (0, 0), rowspan=10)
        self.ax.set_title('机场排队仿真动画演示', fontsize=16, fontweight='bold', pad=15)
        
        # 设置坐标系 - 多列网格布局
        total_width = self.cols * (self.airport_width + 1)
        total_height = self.rows * (self.airport_height + 1)
        
        self.ax.set_xlim(-1, total_width)
        self.ax.set_ylim(-1, total_height)
        self.ax.set_aspect('equal')
        self.ax.axis('off')
        
        # 绘制机场布局
        self.setup_airports()
        
        # 设置控制面板
        self.setup_control_panel()
        
        # 设置统计显示
        self.setup_statistics_display()
        
        # 初始化动态元素
        self.aircraft_objects = {}
        self.queue_displays = {}
        self.statistics_texts = {}
        
    def setup_airports(self):
        """设置机场布局 - 多列网格排列"""
        self.airport_positions = {}
        self.runway_positions = {}
        
        for i, airport in enumerate(self.airports):
            # 多列网格布局
            col = i % self.cols
            row = self.rows - 1 - (i // self.cols)  # 从上到下填充
            
            x_pos = col * (self.airport_width + 1)
            y_pos = row * (self.airport_height + 1)
            self.airport_positions[airport] = (x_pos, y_pos)
            
            # 机场主体区域（左侧小框）
            airport_rect = patches.Rectangle(
                (x_pos, y_pos), 2, self.airport_height,
                linewidth=1.5, edgecolor='navy', facecolor='lightblue', alpha=0.3
            )
            self.ax.add_patch(airport_rect)
            
            # 机场编码（简洁显示）
            self.ax.text(x_pos + 1, y_pos + self.airport_height/2, airport, 
                        ha='center', va='center', fontsize=10, fontweight='bold')
            
            # 起飞跑道（上半部分，蓝色）
            takeoff_y = y_pos + self.airport_height * 0.65
            takeoff_runway = patches.Rectangle(
                (x_pos + 2.2, takeoff_y), self.runway_length, self.runway_width,
                linewidth=1, edgecolor='darkblue', facecolor='blue', alpha=0.6
            )
            self.ax.add_patch(takeoff_runway)
            
            # 降落跑道（下半部分，绿色）  
            landing_y = y_pos + self.airport_height * 0.15
            landing_runway = patches.Rectangle(
                (x_pos + 2.2, landing_y), self.runway_length, self.runway_width,
                linewidth=1, edgecolor='darkgreen', facecolor='green', alpha=0.6
            )
            self.ax.add_patch(landing_runway)
            
            # 存储跑道位置
            self.runway_positions[airport] = {
                'takeoff': (x_pos + 2.2, takeoff_y + self.runway_width/2),
                'landing': (x_pos + 2.2, landing_y + self.runway_width/2)
            }
    
    def setup_control_panel(self):
        """设置控制面板"""
        # 时间滑块
        ax_time = plt.subplot2grid((12, 4), (10, 0), colspan=3)
        self.time_slider = Slider(
            ax_time, 'Time', 
            self.start_time, self.end_time,
            valinit=self.start_time,
            valfmt='%d min'
        )
        self.time_slider.on_changed(self.update_time)
        
        # 播放/暂停按钮
        ax_play = plt.subplot2grid((12, 4), (10, 3))
        self.play_button = Button(ax_play, 'Play/Pause', color='lightgreen')
        self.play_button.on_clicked(self.toggle_play)
        
        # 速度控制
        ax_speed = plt.subplot2grid((12, 4), (11, 0), colspan=2)
        self.speed_slider = Slider(
            ax_speed, 'Speed', 
            0.1, 10,
            valinit=1,
            valfmt='%.1fx'
        )
        self.speed_slider.on_changed(self.update_speed)
        
        # 重置按钮
        ax_reset = plt.subplot2grid((12, 4), (11, 2))
        self.reset_button = Button(ax_reset, 'Reset', color='lightcoral')
        self.reset_button.on_clicked(self.reset_animation)
        
        # 统计按钮
        ax_stats = plt.subplot2grid((12, 4), (11, 3))
        self.stats_button = Button(ax_stats, 'Stats', color='lightblue')
        self.stats_button.on_clicked(self.show_statistics)
    
    def setup_statistics_display(self):
        """设置统计信息显示"""
        # 在右上角显示实时统计
        self.stats_text = self.ax.text(
            0.98, 0.98, '', transform=self.ax.transAxes,
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle="round,pad=0.5", facecolor="white", alpha=0.8),
            fontsize=10, fontfamily='monospace'
        )
    
    def get_current_status(self, current_time):
        """获取当前时间的详细状态"""
        status = defaultdict(lambda: {
            'departures_active': [],
            'arrivals_active': [],
            'departures_queue': [],
            'arrivals_queue': []
        })
        
        active_events = 0
        for event in self.events:
            if event['time'] > current_time:
                break
                
            airport = event['airport']
            aircraft_id = event['aircraft_id']
            activity_type = event['activity_type']
            
            if event['type'] == 'start':
                if event['time'] <= current_time <= event['end_time']:
                    # 活动中的飞机
                    activity_info = {
                        'aircraft_id': aircraft_id,
                        'start_time': event['time'],
                        'end_time': event['end_time'],
                        'delay': event['delay'],
                        'runway': event['runway'],
                        'progress': (current_time - event['time']) / (event['end_time'] - event['time'])
                    }
                    
                    if activity_type == 'departure':
                        status[airport]['departures_active'].append(activity_info)
                    else:
                        status[airport]['arrivals_active'].append(activity_info)
                    active_events += 1
        
        return status
    
    def update_time(self, val):
        """更新时间"""
        self.current_time = self.time_slider.val
        self.update_display()
    
    def update_speed(self, val):
        """更新播放速度"""
        self.animation_speed = self.speed_slider.val
    
    def toggle_play(self, event):
        """切换播放/暂停"""
        self.is_playing = not self.is_playing
        if self.is_playing:
            self.start_animation()
    
    def reset_animation(self, event):
        """重置动画"""
        self.current_time = self.start_time
        self.time_slider.reset()
        self.is_playing = False
        self.update_display()
    
    def start_animation(self):
        """开始自动播放动画"""
        if self.is_playing and self.current_time < self.end_time:
            self.current_time += self.animation_speed
            self.time_slider.set_val(self.current_time)
            self.fig.canvas.draw_idle()
            
            # 继续播放
            self.fig.canvas.flush_events()
            plt.pause(0.1)
            self.start_animation()
    
    def show_statistics(self, event):
        """显示统计信息"""
        print("=== 动画统计信息 ===")
        print(f"时间范围: {int(self.start_time//60):02d}:{int(self.start_time%60):02d} - {int(self.end_time//60):02d}:{int(self.end_time%60):02d}")
        print(f"机场数量: {len(self.airports)}")
        print(f"总活动数: {len(self.simulation_results)}")
        
        # 统计延误情况
        delays = [r['delay_minutes'] for r in self.simulation_results]
        delayed_count = sum(1 for d in delays if d > 0)
        print(f"延误活动: {delayed_count}/{len(delays)} ({delayed_count/len(delays)*100:.1f}%)")
        if delays:
            print(f"平均延误: {np.mean(delays):.1f} 分钟")
            print(f"最大延误: {max(delays):.1f} 分钟")
    
    def clear_dynamic_elements(self):
        """清除动态元素"""
        for obj_list in self.aircraft_objects.values():
            if isinstance(obj_list, list):
                for obj in obj_list:
                    if hasattr(obj, 'remove'):
                        obj.remove()
            else:
                if hasattr(obj_list, 'remove'):
                    obj_list.remove()
        self.aircraft_objects.clear()
    
    def update_display(self):
        """更新显示"""
        # 清除之前的动态元素
        self.clear_dynamic_elements()
        
        # 获取当前状态
        current_status = self.get_current_status(self.current_time)
        
        # 更新每个机场的显示
        total_active = 0
        total_delays = 0
        
        for airport in self.airports:
            active_count = self.update_airport_display(airport, current_status[airport])
            total_active += active_count
            
            # 统计延误
            for dep in current_status[airport]['departures_active']:
                if dep['delay'] > 0:
                    total_delays += 1
            for arr in current_status[airport]['arrivals_active']:
                if arr['delay'] > 0:
                    total_delays += 1
        
        # 更新统计显示
        current_time_str = f"{int(self.current_time//60):02d}:{int(self.current_time%60):02d}"
        stats_text = f"Time: {current_time_str}\nActive Aircraft: {total_active}\nDelayed Aircraft: {total_delays}"
        self.stats_text.set_text(stats_text)
        
        # 更新标题
        self.ax.set_title(f'机场排队仿真动画演示 - 当前时间: {current_time_str}', 
                         fontsize=18, fontweight='bold', pad=20)
        
        # 刷新显示
        self.fig.canvas.draw_idle()
    
    def update_airport_display(self, airport, status):
        """更新单个机场显示"""
        airport_pos = self.airport_positions[airport]
        runway_pos = self.runway_positions[airport]
        active_count = 0
        
        # 显示起飞活动
        for dep in status['departures_active']:
            aircraft_id = dep['aircraft_id']
            progress = max(0, min(1, dep['progress']))
            
            # 计算飞机位置
            start_x = runway_pos['takeoff'][0]
            end_x = runway_pos['takeoff'][0] + self.runway_length
            x_pos = start_x + (end_x - start_x) * progress
            y_pos = runway_pos['takeoff'][1]
            
            # 根据延误情况选择颜色
            if dep['delay'] > 5:
                color = 'red'
            elif dep['delay'] > 0:
                color = 'orange' 
            else:
                color = 'green'
            
            # 绘制飞机（三角形表示起飞方向）
            triangle = patches.RegularPolygon(
                (x_pos, y_pos), 3, radius=0.15, 
                orientation=0, facecolor=color, edgecolor='black', alpha=0.8
            )
            self.ax.add_patch(triangle)
            
            # 添加飞机标签（简化）
            text = self.ax.text(x_pos, y_pos + 0.3, aircraft_id[-4:], 
                              ha='center', va='center', fontsize=7, fontweight='bold')
            
            if airport not in self.aircraft_objects:
                self.aircraft_objects[airport] = []
            self.aircraft_objects[airport].extend([triangle, text])
            active_count += 1
        
        # 显示降落活动
        for arr in status['arrivals_active']:
            aircraft_id = arr['aircraft_id']
            progress = max(0, min(1, arr['progress']))
            
            # 计算飞机位置（从右到左）
            start_x = runway_pos['landing'][0] + self.runway_length
            end_x = runway_pos['landing'][0]
            x_pos = start_x + (end_x - start_x) * progress
            y_pos = runway_pos['landing'][1]
            
            # 根据延误情况选择颜色
            if arr['delay'] > 5:
                color = 'red'
            elif arr['delay'] > 0:
                color = 'orange'
            else:
                color = 'blue'
            
            # 绘制飞机（三角形表示降落方向）
            triangle = patches.RegularPolygon(
                (x_pos, y_pos), 3, radius=0.15, 
                orientation=np.pi, facecolor=color, edgecolor='black', alpha=0.8
            )
            self.ax.add_patch(triangle)
            
            # 添加飞机标签（简化）
            text = self.ax.text(x_pos, y_pos - 0.3, aircraft_id[-4:], 
                              ha='center', va='center', fontsize=7, fontweight='bold')
            
            if airport not in self.aircraft_objects:
                self.aircraft_objects[airport] = []
            self.aircraft_objects[airport].extend([triangle, text])
            active_count += 1
        
        return active_count

def create_enhanced_animation_demo():
    """创建增强版动画演示"""
    print("=== 创建增强版机场排队动画演示 ===")
    
    # 使用更有趣的配置 - 增加延误和冲突
    simulator = AirportQueueSimulator(
        departure_time=20,  # 增加出港时间到20分钟
        arrival_time=10,     # 增加入港时间到10分钟
        num_runways=10       # 增加跑道数量到10条
    )
    
    # 加载飞行计划
    simulator.load_flight_plans("仿真/all_flight_plans.xml")
    
    # 收集机场活动
    airport_activities = simulator.collect_airport_activities()
    
    # 执行仿真，加入更大的随机延误
    updated_plans = simulator.simulate_queue(airport_activities, time_disturbance=10)  # 增加随机延误到10分钟
    
    # 获取所有活动的时间范围
    all_times = [simulator.parse_time_string(r['actual_start']) for r in simulator.simulation_results]
    if not all_times:
        print("⚠️ 没有找到任何航班活动！")
        return None
        
    start_time = min(all_times)
    end_time = max(all_times)  # 显示所有活动，不限制时间窗口
    
    print(f"动画时间窗口: {simulator.minutes_to_time_string(start_time)} - {simulator.minutes_to_time_string(end_time)}")
    print(f"总共 {len(simulator.simulation_results)} 个活动")
    
    # 创建增强版动画
    animation = EnhancedAirportQueueAnimation(
        simulator.simulation_results,
        simulator.flight_plans,
        start_time,
        end_time
    )
    
    # 显示动画
    plt.rcParams['font.size'] = 10
    animation.update_display()
    plt.tight_layout()
    plt.show()
    
    return animation

if __name__ == "__main__":
    try:
        animation = create_enhanced_animation_demo()
        print("✅ 增强版动画创建成功！")
        print("🎮 使用控制面板的功能：")
        print("   - 拖动时间滑块查看不同时刻")
        print("   - 点击Play/Pause自动播放")
        print("   - 调整Speed控制播放速度")
        print("   - 点击Reset重置到开始")
        print("   - 点击Stats查看统计信息")
    except Exception as e:
        print(f"❌ 创建增强版动画时出错: {e}")
        import traceback
        traceback.print_exc()
