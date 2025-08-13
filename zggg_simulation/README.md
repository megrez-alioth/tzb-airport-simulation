# ZGGG机场仿真系统使用说明

## 系统概述

ZGGG机场仿真系统是专为广州白云国际机场设计的航班运行仿真工具，能够模拟机场的出港和入港操作，分析跑道利用率、延误情况和积压时段，并提供参数优化功能。

## 主要功能

### 1. 航班仿真
- **出港仿真**: 模拟航班从停机位到起飞的完整流程
- **入港仿真**: 模拟航班从降落到停机位的完整流程
- **跑道调度**: 基于ICAO标准的尾流间隔管理
- **延误计算**: 精确计算延误时间和原因分析

### 2. 数据处理
- **Excel数据导入**: 支持标准航班数据格式
- **ZGGG数据提取**: 自动识别和提取ZGGG相关航班
- **数据清洗**: 处理缺失值和异常数据
- **时间标准化**: 统一时间格式和时区处理

### 3. 验证系统
- **四项核心指标**: 符合行业标准的验证指标
- **统计分析**: 延误分布、正点率等关键指标
- **对比验证**: 仿真结果与实际数据对比
- **报告生成**: 详细的验证报告和Excel导出

### 4. 参数优化
- **自动优化**: 基于验证指标的参数自动调优
- **多目标优化**: 平衡延误、正点率等多个目标
- **配置管理**: 灵活的参数配置和保存
- **历史追踪**: 优化过程的完整记录

## 系统架构

```
zggg_simulation/
├── core/                    # 核心仿真模块
│   ├── parameters.py        # 配置参数管理
│   ├── aircraft_classifier.py  # 飞机分类器
│   ├── runway_scheduler.py  # 跑道调度器
│   └── zggg_simulator.py    # 主仿真引擎
├── data/                    # 数据处理模块
│   └── data_loader.py       # 数据加载器
├── utils/                   # 工具模块
│   └── time_utils.py        # 时间处理工具
├── validation/              # 验证模块
│   ├── metrics_calculator.py   # 指标计算器
│   └── result_validator.py     # 结果验证器
├── main_zggg_simulation.py # 主执行脚本
├── requirements.txt         # 依赖配置
└── README.md               # 使用说明
```

## 安装和配置

### 1. 环境要求
- Python 3.8 或更高版本
- 至少8GB内存（处理大规模数据时）
- 硬盘空间至少1GB用于存储结果

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

### 3. 数据准备
将航班数据文件放置在 `数据/` 目录下，确保文件名为：
- `5月航班运行数据（实际数据列）.xlsx`

数据文件应包含以下列（中文列名）：
- 航班号、计划起飞时间、实际起飞时间
- 起飞机场、到达机场
- 机型、飞机注册号
- 其他相关字段

## 快速开始

### 1. 基本使用
```python
from zggg_simulation import ZGGGSimulationSystem

# 创建仿真系统
sim_system = ZGGGSimulationSystem('数据/5月航班运行数据（实际数据列）.xlsx')

# 加载数据
sim_system.load_and_prepare_data()

# 运行仿真
results = sim_system.run_single_simulation()
```

### 2. 参数定制
```python
# 自定义配置
config_override = {
    'min_departure_rot': 120,  # 出港ROT调整为120秒
    'min_arrival_rot': 90,     # 入港ROT调整为90秒
    'taxi_buffer_minutes': 15  # 滑行缓冲时间15分钟
}

sim_system = ZGGGSimulationSystem(data_file, config_override)
```

### 3. 参数优化
```python
# 自动寻找最佳参数
optimization_results = sim_system.run_optimization_loop(max_iterations=20)
best_config = optimization_results['best_config']
```

## 命令行使用

### 1. 直接运行
```bash
cd zggg_simulation
python main_zggg_simulation.py
```

### 2. 交互式选择
程序启动后会提供以下选项：
- **选项1**: 单次仿真（使用默认参数）
- **选项2**: 参数优化（自动寻找最佳参数）
- **选项3**: 退出程序

### 3. 运行示例
```
ZGGG机场仿真系统
==================================================
正在加载数据...
成功加载 15432 条原始航班数据
成功提取 2856 条ZGGG航班数据
数据预处理完成，有效数据 2743 条

请选择运行模式:
1. 单次仿真 (使用默认参数)
2. 参数优化 (自动寻找最佳参数)  
3. 退出

请输入选择 (1-3): 1
```

## 配置参数说明

### 1. 跑道配置
```python
runway_config = {
    '02L': {'type': 'departure'},   # 02L跑道用于出港
    '02R': {'type': 'departure'},   # 02R跑道用于出港  
    '20L': {'type': 'arrival'},     # 20L跑道用于入港
    '20R': {'type': 'arrival'}      # 20R跑道用于入港
}
```

### 2. 时间参数
- **min_departure_rot**: 出港最小跑道占用时间（秒）
- **min_arrival_rot**: 入港最小跑道占用时间（秒）
- **taxi_buffer_minutes**: 滑行缓冲时间（分钟）

### 3. 尾流间隔（ICAO标准）
- Heavy-Heavy: 90秒
- Heavy-Medium: 120秒
- Heavy-Light: 180秒
- Medium-Light: 120秒

### 4. 验证阈值
- **period_tolerance_minutes**: 积压时段偏差容忍度（60分钟）
- **peak_deviation_threshold**: 高峰偏差阈值（15%）
- **duration_tolerance_ratio**: 持续时间容忍比例（0.3）

## 结果输出

### 1. Excel报告
仿真完成后自动生成包含以下工作表的Excel文件：
- **出港仿真结果**: 详细的出港航班仿真数据
- **入港仿真结果**: 详细的入港航班仿真数据  
- **积压时段分析**: 积压时段的统计分析
- **验证指标**: 四项核心验证指标结果

### 2. JSON配置
保存使用的配置参数，便于结果重现：
```json
{
  "runway_config": {...},
  "time_parameters": {...},
  "wake_separation": {...},
  "validation_thresholds": {...}
}
```

### 3. 控制台输出
实时显示仿真进度和关键指标：
```
=== ZGGG仿真验证报告 ===
指标1 - 积压时段偏差: 通过 ✓
指标2 - 持续时间一致性: 通过 ✓  
指标3 - 高峰偏差: 未通过 ✗ (偏差18.5% > 15%)
指标4 - 最晚运营时间: 通过 ✓

总体通过率: 75% (3/4)
```

## 验证指标详解

### 指标1: 积压时段偏差
- **目标**: 仿真预测的积压时段与实际相比偏差不超过1小时
- **计算**: |仿真开始时间 - 实际开始时间| ≤ 60分钟
- **通过条件**: 80%以上的积压时段满足偏差要求

### 指标2: 持续时间一致性  
- **目标**: 积压时段的持续时间预测准确
- **计算**: |仿真持续时间 - 实际持续时间| / 实际持续时间 ≤ 30%
- **通过条件**: 80%以上的积压时段满足一致性要求

### 指标3: 高峰偏差
- **目标**: 高峰时段航班量预测偏差不超过15%
- **计算**: |仿真高峰量 - 实际高峰量| / 实际高峰量 ≤ 15%
- **通过条件**: 所有高峰时段均满足偏差要求

### 指标4: 最晚运营时间
- **目标**: 最晚航班运营时间预测准确
- **计算**: |仿真最晚时间 - 实际最晚时间| ≤ 30分钟
- **通过条件**: 满足时间偏差要求

## 高级功能

### 1. 批量处理
```python
# 处理多个数据文件
data_files = ['数据1.xlsx', '数据2.xlsx', '数据3.xlsx']

for file in data_files:
    sim_system = ZGGGSimulationSystem(file)
    sim_system.load_and_prepare_data()
    results = sim_system.run_single_simulation()
    print(f"{file} 处理完成")
```

### 2. 自定义验证指标
```python
# 自定义目标指标
target_metrics = {
    'min_passed_metrics': 4,    # 要求4个指标全部通过
    'max_mean_delay': 20.0,     # 平均延误不超过20分钟
    'min_on_time_rate': 80.0    # 正点率不低于80%
}

results = sim_system.run_optimization_loop(target_metrics=target_metrics)
```

### 3. 性能监控
```python
# 启用详细日志
import logging
logging.basicConfig(level=logging.DEBUG)

# 性能分析
import time
start_time = time.time()
results = sim_system.run_single_simulation()
print(f"仿真耗时: {time.time() - start_time:.2f}秒")
```

## 故障排除

### 1. 常见错误

**数据文件未找到**
```
错误: 数据文件不存在 - /path/to/data.xlsx
解决: 检查文件路径和文件名，确保数据文件存在
```

**内存不足**
```
MemoryError: Unable to allocate array
解决: 减少数据量或增加系统内存，考虑分批处理大数据
```

**编码错误**  
```
UnicodeDecodeError: 'utf-8' codec can't decode
解决: 确保Excel文件格式正确，必要时重新保存文件
```

### 2. 性能优化
- **数据预处理**: 删除不必要的列以减少内存使用
- **分批处理**: 对于大规模数据，考虑按时间段分批处理
- **并行计算**: 利用多核CPU加速计算（未来版本功能）

### 3. 调试技巧
```python
# 启用调试模式
sim_system = ZGGGSimulationSystem(data_file)
sim_system.config.debug_mode = True

# 查看中间结果
sim_system.load_and_prepare_data()
print(f"ZGGG数据量: {len(sim_system.zggg_data)}")
print("前5条数据:")
print(sim_system.zggg_data.head())
```

## 更新日志

### 版本 1.0.0 (当前版本)
- 初始版本发布
- 支持ZGGG机场仿真的基本功能
- 四项验证指标系统
- 参数优化功能
- Excel数据处理和报告导出

### 计划功能 (未来版本)
- 图形化界面支持
- 实时仿真监控  
- 更多机场的适配
- 天气因素集成
- 机器学习预测模块

## 技术支持

如遇到问题或需要技术支持，请提供：
1. 错误信息的完整截图
2. 使用的数据文件格式和大小
3. 系统环境信息（Python版本、操作系统）
4. 复现错误的步骤

## 许可证

本软件仅用于学术研究和教育目的。商业使用需要额外授权。

---

*ZGGG机场仿真系统 v1.0.0*  
*Copyright © 2024 ZGGG Simulation Team*
