#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速机场动画演示 - 将所有活动压缩到更短时间窗口内
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
from datetime import datetime, timedelta
import random
import xml.etree.ElementTree as ET
from matplotlib.widgets import Slider, Button
import warnings
warnings.filterwarnings("ignore")

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

class FastDemoAnimation:
    def __init__(self):
        # 创建压缩的演示数据
        self.create_demo_data()
        
    def create_demo_data(self):
        """创建压缩的演示数据"""
        self.airports = ['ZGGG', 'OMDB', 'YSSY', 'ZGSZ', 'ZUUU', 'ZLLL']
        self.aircraft_types = ['B1062', 'B1092', 'B1063', 'B1065', 'B1121']
        
        # 将所有活动压缩到60分钟内
        self.activities = []
        
        # 为每个机场生成一些活动
        current_time = 0  # 从0分钟开始
        
        for i, airport in enumerate(self.airports):
            # 每个机场生成2-4个起飞降落活动
            num_activities = random.randint(2, 4)
            
            for j in range(num_activities):
                aircraft = random.choice(self.aircraft_types)
                
                # 起飞活动
                start_time = current_time + j * 8 + random.randint(0, 3)
                end_time = start_time + random.randint(2, 4)  # 2-4分钟
                
                self.activities.append({
                    'aircraft': aircraft,
                    'airport': airport,
                    'activity_type': 'departure',
                    'start_time': start_time,
                    'end_time': end_time,
                    'runway': 1,
                    'delay': random.randint(0, 2)
                })
                
                # 降落活动
                start_time = current_time + j * 8 + random.randint(4, 6)
                end_time = start_time + random.randint(1, 3)  # 1-3分钟
                
                self.activities.append({
                    'aircraft': aircraft,
                    'airport': airport,
                    'activity_type': 'arrival',
                    'start_time': start_time,
                    'end_time': end_time,
                    'runway': 1,
                    'delay': random.randint(0, 1)
                })
            
            current_time += 10  # 每个机场间隔10分钟
        
        # 按时间排序
        self.activities.sort(key=lambda x: x['start_time'])
        
        self.start_time = 0
        self.end_time = max(activity['end_time'] for activity in self.activities)
        
        print(f"创建演示数据: {len(self.activities)} 个活动")
        print(f"时间范围: {self.start_time}-{self.end_time} 分钟")
        
        # 显示前几个活动
        for i, activity in enumerate(self.activities[:10]):
            print(f"  {i+1}. {activity['aircraft']} 在 {activity['airport']} "
                  f"{activity['activity_type']} {activity['start_time']}-{activity['end_time']}分钟")
    
    def setup_animation(self):
        """设置动画界面"""
        self.fig, self.axes = plt.subplots(2, 3, figsize=(15, 10))
        self.fig.suptitle('机场排队动画演示 - 快速版', fontsize=16, fontweight='bold')
        
        # 扁平化axes以便索引
        self.ax_list = self.axes.flatten()
        
        # 机场位置和颜色
        colors = plt.cm.Set3(np.linspace(0, 1, len(self.airports)))
        
        # 为每个机场设置子图
        for i, (airport, ax) in enumerate(zip(self.airports, self.ax_list)):
            ax.set_xlim(0, 10)
            ax.set_ylim(0, 6)
            ax.set_title(f'{airport}', fontsize=12, fontweight='bold')
            ax.set_aspect('equal')
            
            # 绘制跑道
            departure_runway = patches.Rectangle((1, 4), 8, 0.5, 
                                               facecolor='blue', alpha=0.7, 
                                               label='起飞跑道')
            arrival_runway = patches.Rectangle((1, 2), 8, 0.5, 
                                             facecolor='green', alpha=0.7, 
                                             label='降落跑道')
            
            ax.add_patch(departure_runway)
            ax.add_patch(arrival_runway)
            
            # 添加标签
            ax.text(5, 4.7, '起飞', ha='center', va='center', fontsize=10)
            ax.text(5, 2.7, '降落', ha='center', va='center', fontsize=10)
            
            ax.set_xticks([])
            ax.set_yticks([])
        
        # 添加控制面板
        self.add_controls()
        
        # 初始化飞机存储
        self.aircraft_patches = {}
        
        plt.tight_layout()
    
    def add_controls(self):
        """添加控制面板"""
        # 为控制面板留出空间
        plt.subplots_adjust(bottom=0.25)
        
        # 时间滑块
        ax_time = plt.axes([0.2, 0.15, 0.5, 0.03])
        self.time_slider = Slider(ax_time, '时间 (分钟)', 
                                 self.start_time, self.end_time, 
                                 valinit=self.start_time, valstep=0.5)
        self.time_slider.on_changed(self.update_animation)
        
        # 播放控制按钮
        ax_play = plt.axes([0.1, 0.05, 0.1, 0.04])
        self.play_button = Button(ax_play, 'Play')
        self.play_button.on_clicked(self.toggle_play)
        
        # 重置按钮
        ax_reset = plt.axes([0.25, 0.05, 0.1, 0.04])
        self.reset_button = Button(ax_reset, 'Reset')
        self.reset_button.on_clicked(self.reset_animation)
        
        # 速度控制
        ax_speed = plt.axes([0.4, 0.05, 0.2, 0.03])
        self.speed_slider = Slider(ax_speed, '速度', 0.1, 2.0, valinit=1.0)
        
        # 统计按钮
        ax_stats = plt.axes([0.75, 0.05, 0.1, 0.04])
        self.stats_button = Button(ax_stats, 'Stats')
        self.stats_button.on_clicked(self.show_stats)
        
        # 播放状态
        self.is_playing = False
    
    def get_current_activities(self, current_time):
        """获取当前时间的活动"""
        active = []
        for activity in self.activities:
            if activity['start_time'] <= current_time <= activity['end_time']:
                active.append(activity)
        return active
    
    def update_animation(self, val=None):
        """更新动画"""
        if val is None:
            current_time = self.time_slider.val
        else:
            current_time = val
        
        # 清除之前的飞机
        for patches_list in self.aircraft_patches.values():
            for patch in patches_list:
                patch.remove()
        self.aircraft_patches.clear()
        
        # 获取当前活动
        current_activities = self.get_current_activities(current_time)
        
        print(f"时间 {current_time:.1f}: {len(current_activities)} 个活动中的飞机")
        
        # 为每个活动绘制飞机
        for activity in current_activities:
            airport = activity['airport']
            if airport not in self.airports:
                continue
                
            airport_idx = self.airports.index(airport)
            ax = self.ax_list[airport_idx]
            
            # 根据活动类型确定位置
            if activity['activity_type'] == 'departure':
                y_pos = 4.25
                color = 'red'
            else:  # arrival
                y_pos = 2.25
                color = 'orange'
            
            # 计算飞机在跑道上的位置（基于时间进度）
            progress = (current_time - activity['start_time']) / (activity['end_time'] - activity['start_time'])
            x_pos = 1 + progress * 8
            
            # 创建飞机三角形
            triangle = patches.RegularPolygon((x_pos, y_pos), 3, radius=0.3, 
                                            orientation=0 if activity['activity_type'] == 'departure' else np.pi,
                                            facecolor=color, alpha=0.8)
            
            ax.add_patch(triangle)
            
            # 存储patch以便后续清除
            if airport not in self.aircraft_patches:
                self.aircraft_patches[airport] = []
            self.aircraft_patches[airport].append(triangle)
            
            # 添加飞机标签
            text = ax.text(x_pos, y_pos + 0.5, activity['aircraft'], 
                          ha='center', va='center', fontsize=8)
            self.aircraft_patches[airport].append(text)
        
        # 更新显示
        self.fig.canvas.draw_idle()
    
    def toggle_play(self, event):
        """切换播放状态"""
        self.is_playing = not self.is_playing
        if self.is_playing:
            self.play_button.label.set_text('Pause')
            self.auto_play()
        else:
            self.play_button.label.set_text('Play')
    
    def auto_play(self):
        """自动播放"""
        if not self.is_playing:
            return
            
        current_time = self.time_slider.val
        speed = self.speed_slider.val
        
        if current_time >= self.end_time:
            self.is_playing = False
            self.play_button.label.set_text('Play')
            return
        
        # 更新时间
        new_time = min(current_time + 0.5 * speed, self.end_time)
        self.time_slider.set_val(new_time)
        
        # 继续播放
        if self.is_playing:
            self.fig.canvas.mpl_connect('draw_event', 
                                       lambda x: self.fig.canvas.start_event_loop(0.1))
            self.fig.after_idle(lambda: self.auto_play())
    
    def reset_animation(self, event):
        """重置动画"""
        self.is_playing = False
        self.play_button.label.set_text('Play')
        self.time_slider.set_val(self.start_time)
    
    def show_stats(self, event):
        """显示统计信息"""
        current_time = self.time_slider.val
        current_activities = self.get_current_activities(current_time)
        
        stats_text = f"当前时间: {current_time:.1f} 分钟\n"
        stats_text += f"活动中飞机: {len(current_activities)} 架\n\n"
        
        # 按机场统计
        airport_stats = {}
        for activity in current_activities:
            airport = activity['airport']
            if airport not in airport_stats:
                airport_stats[airport] = {'departure': 0, 'arrival': 0}
            airport_stats[airport][activity['activity_type']] += 1
        
        for airport, stats in airport_stats.items():
            stats_text += f"{airport}: 起飞{stats['departure']} 降落{stats['arrival']}\n"
        
        print(stats_text)
        plt.figtext(0.02, 0.02, stats_text, fontsize=10, 
                   bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow"))
        self.fig.canvas.draw()
    
    def run(self):
        """运行动画"""
        self.setup_animation()
        self.update_animation(self.start_time)
        plt.show()

def create_fast_demo():
    """创建快速演示动画"""
    print("=== 创建快速机场动画演示 ===")
    
    animation = FastDemoAnimation()
    animation.run()
    
    print("✅ 快速演示动画创建成功！")
    print("🎮 使用控制面板的功能：")
    print("   - 拖动时间滑块查看不同时刻")
    print("   - 点击Play/Pause自动播放")
    print("   - 调整Speed控制播放速度")
    print("   - 点击Reset重置到开始")
    print("   - 点击Stats查看统计信息")

if __name__ == "__main__":
    create_fast_demo()
