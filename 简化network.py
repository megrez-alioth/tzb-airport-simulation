import xml.etree.ElementTree as ET
import os


def parse_plans_file(plans_file):
    """
    解析plans文件，提取所有使用的航站
    """
    print(f"正在解析plans文件: {plans_file}")
    
    if not os.path.exists(plans_file):
        print(f"错误: 文件 {plans_file} 不存在")
        return None
    
    try:
        tree = ET.parse(plans_file)
        root = tree.getroot()
    except Exception as e:
        print(f"解析plans文件时出错: {e}")
        return None
    
    used_airports = set()
    used_links = set()
    
    # 遍历所有person和activity
    for person in root.findall('person'):
        for plan in person.findall('plan'):
            for act in plan.findall('act'):
                link = act.get('link')
                if link:
                    used_links.add(link)
                    # 从link中提取航站（格式：AIRPORT1-AIRPORT2）
                    if '-' in link:
                        parts = link.split('-')
                        if len(parts) == 2:
                            airport1, airport2 = parts
                            used_airports.add(airport1)
                            used_airports.add(airport2)
    
    print(f"发现 {len(used_airports)} 个使用的航站")
    print(f"发现 {len(used_links)} 个使用的航线")
    
    # 打印使用的航站
    print("使用的航站:")
    for i, airport in enumerate(sorted(used_airports), 1):
        print(f"{i:2d}. {airport}")
    
    return used_airports, used_links

def parse_network_file(network_file):
    """
    解析network文件
    """
    print(f"\n正在解析network文件: {network_file}")
    
    if not os.path.exists(network_file):
        print(f"错误: 文件 {network_file} 不存在")
        return None
    
    try:
        tree = ET.parse(network_file)
        root = tree.getroot()
    except Exception as e:
        print(f"解析network文件时出错: {e}")
        return None
    
    return tree, root

def create_simplified_network(network_tree, network_root, used_airports, used_links, output_file):
    """
    创建简化的network文件
    """
    print(f"\n正在创建简化的network文件: {output_file}")
    
    # 获取原始节点和链接
    nodes_element = network_root.find('nodes')
    links_element = network_root.find('links')
    
    if nodes_element is None or links_element is None:
        print("错误: network文件格式不正确")
        return
    
    # 统计原始数据
    original_nodes = nodes_element.findall('node')
    original_links = links_element.findall('link')
    
    print(f"原始节点数: {len(original_nodes)}")
    print(f"原始链接数: {len(original_links)}")
    
    # 创建新的network结构
    new_root = ET.Element('network')
    new_root.set('name', 'simplified aviation network')
    
    # 创建nodes元素
    new_nodes = ET.SubElement(new_root, 'nodes')
    
    # 添加使用的节点
    kept_nodes = 0
    for node in original_nodes:
        node_id = node.get('id')
        if node_id in used_airports:
            new_node = ET.SubElement(new_nodes, 'node')
            new_node.set('id', node_id)
            new_node.set('x', node.get('x'))
            new_node.set('y', node.get('y'))
            kept_nodes += 1
    
    # 创建links元素
    new_links = ET.SubElement(new_root, 'links')
    
    # 复制原始links的属性
    for attr_name, attr_value in links_element.attrib.items():
        new_links.set(attr_name, attr_value)
    
    # 只添加在plans中实际使用的航线
    kept_links = 0
    for link in original_links:
        link_id = link.get('id')
        
        # 检查这条航线是否在plans文件中被使用
        if link_id in used_links:
            new_link = ET.SubElement(new_links, 'link')
            # 复制所有属性
            for attr_name, attr_value in link.attrib.items():
                new_link.set(attr_name, attr_value)
            kept_links += 1
    
    print(f"保留节点数: {kept_nodes}")
    print(f"保留链接数: {kept_links}")
    print(f"删除节点数: {len(original_nodes) - kept_nodes}")
    print(f"删除链接数: {len(original_links) - kept_links}")
    
    # 验证数据一致性
    print(f"\n=== 数据验证 ===")
    print(f"Plans中使用的航线数: {len(used_links)}")
    print(f"Network中保留的航线数: {kept_links}")
    
    if kept_links != len(used_links):
        print("⚠️  警告：保留的航线数与使用的航线数不匹配")
        # 找出缺失的航线
        found_links = set()
        for link in original_links:
            if link.get('id') in used_links:
                found_links.add(link.get('id'))
        
        missing_links = used_links - found_links
        if missing_links:
            print(f"在network中找不到的航线: {missing_links}")
    else:
        print("✅ 数据一致性验证通过")
    
    # 保存新的network文件
    try:
        # 创建新的树
        new_tree = ET.ElementTree(new_root)
        
        # 格式化XML
        def indent(elem, level=0):
            i = "\n" + level * "   "
            if len(elem):
                if not elem.text or not elem.text.strip():
                    elem.text = i + "   "
                if not elem.tail or not elem.tail.strip():
                    elem.tail = i
                for child in elem:
                    indent(child, level + 1)
                if not child.tail or not child.tail.strip():
                    child.tail = i
            else:
                if level and (not elem.tail or not elem.tail.strip()):
                    elem.tail = i
        
        indent(new_root)
        
        # 写入文件
        with open(output_file, 'wb') as f:
            f.write(b'<?xml version="1.0" encoding="utf-8"?>\n')
            f.write(b'<!DOCTYPE network SYSTEM "http://www.matsim.org/files/dtd/network_v1.dtd">\n')
            f.write(b'\n')
            new_tree.write(f, encoding='utf-8', xml_declaration=False)
        
        print(f"✅ 简化的network文件已保存: {output_file}")
        
    except Exception as e:
        print(f"❌ 保存文件时出错: {e}")

def generate_statistics(used_airports, used_links):
    """
    生成统计报告
    """
    print(f"\n=== 统计报告 ===")
    print(f"使用的航站数量: {len(used_airports)}")
    print(f"使用的航线数量: {len(used_links)}")
    
    # 计算每个航站的使用频率
    airport_usage = {}
    for link in used_links:
        if '-' in link:
            parts = link.split('-')
            if len(parts) == 2:
                airport1, airport2 = parts
                airport_usage[airport1] = airport_usage.get(airport1, 0) + 1
                airport_usage[airport2] = airport_usage.get(airport2, 0) + 1
    
    # 按使用频率排序
    sorted_airports = sorted(airport_usage.items(), key=lambda x: x[1], reverse=True)
    
    print(f"\n=== 航站使用频率排序（前10个）===")
    for i, (airport, count) in enumerate(sorted_airports[:10], 1):
        print(f"{i:2d}. {airport}: {count} 次")

def main():
    """
    主函数
    """
    # 文件路径
    plans_file = "/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/仿真/flight_plans.xml"
    network_file = "/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/仿真/network.xml"
    output_file = "/Users/megrez/Library/Mobile Documents/com~apple~CloudDocs/BUAA/科研/挑战杯/航空挑战杯/仿真/network_simplified.xml"
    
    print("=" * 60)
    print("Network文件简化工具")
    print("=" * 60)
    
    # 解析plans文件，获取使用的航站
    result = parse_plans_file(plans_file)
    if result is None:
        print("无法解析plans文件，程序退出")
        return
    
    used_airports, used_links = result
    
    # 解析network文件
    network_result = parse_network_file(network_file)
    if network_result is None:
        print("无法解析network文件，程序退出")
        return
    
    network_tree, network_root = network_result
    
    # 创建简化的network文件
    create_simplified_network(network_tree, network_root, used_airports, used_links, output_file)
    
    # 生成统计报告
    generate_statistics(used_airports, used_links)
    
    print(f"\n=== 处理完成 ===")
    print(f"原始network文件: {network_file}")
    print(f"简化后文件: {output_file}")
    print(f"现在可以使用简化后的network文件进行仿真，视觉效果会更清爽！")

if __name__ == "__main__":
    main()
