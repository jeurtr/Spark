# -*- coding: utf-8 -*-
#
# Spark ETL: jm大道及其路口识别
#
# spark-submit --master local[4] road_etl.py "$source_filename" "$target_path"
#
# Author: alex
# Created Time: 2017年07月11日 星期二 10时07分03秒
import re
import sys
from time import localtime
from pyspark import SparkContext, SparkConf
from functools import cmp_to_key

# ******************* 配置 ******************************

# 字段下标定义
uid_index = 0      # 用户ID字段
hour_index = 1     # 小时段
start_index = 2    # 开始时间字段，整型
end_index = 3      # 结束时间字段，整型
station_index = 4  # 基站字段（经过格式化的）
lon_index = 5      # 经度字段
lat_index = 6      # 纬度字段
county_index = 7   # 区县字段
town_index = 8     # 镇名字段
is_street_index = 9        # 是否街道，是or否
station_type_index = 10    # 基站类型：宏站
cover_area_index = 11      # 覆盖类型：室内，室外，室内外
cover_scene_index = 12     # 覆盖场景: 城区道路等
road_index = 13            # 道路字段
cross_index = 14           # 路口字段

# 清洗时新增字段
station_ex_index = 15          # 完整的基站字段
consume_time_index = 16        # int((end_time - start_time) / 60), 单位：分钟
interval_time_index = 17       # 前后记录的时间间隔（两条记录合并时，中间可能有时间间隔）

# 分析结果字段
real_road_index = 18       # 分析后的道路字段：经过道路旁边的基站并不一定就在道路上
cross_status_index = 19    # 路口状态字段：0表示不是经过路口，1表示出，2表示入

# 道路上基站允许的时间差
# 车在道路上行驶，可能会有掉线的可能
time_diff = 10

# 常量定义
jm_load_id = 1   # 道路id定义

# 道路出入口状态常量定义
cross_default = 0  # 路口的默认状态
cross_in = 1       # 道路入口(进入道路)
cross_out = 2      # 道路出口(从道路离开)
cross_stay = 3     # 在路口停留（可能是办事，或者工作，或者回家等）
cross_pass = 4     # 只是在路口路过，没有出去
cross_unknown = 5  # 未知状态

# 基站名称格式化
station_format = re.compile("[A-Z\-\d]+$")

# ******************* 配置 ******************************


def main():
    # 格式化数据
    conf = SparkConf().setAppName('JM City ETL')
    sc = SparkContext(conf=conf)
    source = sc.textFile(source_filename)
    mapped_data = source.map(init_map)

    # 测试阶段减少数据量
    # mapped_data = mapped_data.filter(lambda x: x[0].endswith('3'))

    # 按用户ID聚合数据
    grouped_data = mapped_data.groupBy(lambda x: x[uid_index])
    filter_data = grouped_data.filter(road_filter)
    filter_data = filter_data.coalesce(100).cache()

    road_data = filter_data.flatMap(load_map)

    road_data.map(save_map).saveAsTextFile(target_path)
    sc.stop()
    print("*"*40)


def save_map(row):
    """将数组格式化成csv的格式"""
    return ','.join((str(w) for w in row))


def init_map(line):
    """格式化数据：字段类型等"""
    line = line.split(',')
    line[road_index] = line[road_index].replace("\\", '')
    line[cross_index] = line[cross_index].replace("\\", '')

    line[start_index] = int(line[start_index])
    line[end_index] = int(line[end_index])
    line[road_index] = int(line[road_index]) if line[road_index] != 'N' else 0

    # 基站字段处理
    line.append(line[station_index])
    line[station_index] = station_format.sub('', line[station_index]).strip()
    return line


def road_filter(grouped_row):
    """根据3个连续在jm达到上过滤相应数据
    99%的数据都会被过滤掉"""
    row = list(grouped_row[1])    # 二维数组
    continue_road_cnt, continue_road_max = 0, 0
    for i in row:
        if i[road_index] == jm_load_id:
            continue_road_cnt += 1
        else:
            continue_road_cnt = 0

        if continue_road_cnt > continue_road_max:
            continue_road_max = continue_road_cnt

    return continue_road_max > 2


def user_cross_map(data):
    """路口识别
    1. 经过路口，但没有出去：经过路口之后，后续时刻还是在jm大道上
    2. 从路口出去: 经过路口之后，后续就没出现在jm大道上
    3. 出错路口，绕一下又回去的
    4. TODO 入口基站如果跟出口基站有重叠的话，就不应该算是上了jm大道
    """
    l = len(data)
    new_data = []
    continue_cnt = 0
    for i in range(0, l):
        if continue_cnt > 0:
            continue_cnt -= 1
            continue

        i_row = data[i]
        if i_row[cross_index] == 'N':
            # 如果不是路口的基站
            i_row.append(cross_default)
            new_data.append(i_row)
            continue

        end_time = i_row[end_index]
        for j in range(i+1, l):
            if data[j][cross_index] != 'N':
                # 如果基站在某个路口上
                continue_cnt += 1
                end_time = data[j][end_index]
            else:
                break

        # 该路口之前的状态
        before_is_road = False    # 之前是在jm大道上
        if i > 0 and data[i-1][real_road_index] == jm_load_id:
            before_is_road = True

        # 该路口之后的状态
        after_is_road = False     # 之后是在jm大道上
        j = i + continue_cnt
        if j < l-1 and data[j+1][real_road_index] == jm_load_id:
            after_is_road = True

        # 判断路口状态
        consume_time = end_time - i_row[start_index]
        if before_is_road and after_is_road:
            # 前后都在jm大道上
            if consume_time > 1800:
                # 停留超过半小时的
                cross_status = cross_stay
            else:
                # 只是路过，都修改为在路上
                cross_status = cross_pass   # 只是路过一下
                for k in range(i, j+1):
                    data[k][real_road_index] = jm_load_id

        elif before_is_road:
            cross_status = cross_out  # 道路出口
            # 判断出入口的基站是否有交集，在jm路上的时间是否超过1.5分钟
        elif after_is_road:
            cross_status = cross_in   # 道路入口
        else:  # 前后都不在jm大道上
            cross_status = cross_unknown

        for k in range(i, j+1):  # 记录路口的状态
            row = data[k]
            row.append(cross_status)
            new_data.append(row)

    nl = len(new_data)
    if l != nl:
        print("==> ERROR: user cross map %d != %d" % (l, nl))
        print(data)
        print(new_data)

    return new_data


def user_road_map(data):
    """道路识别:
        1. 连续经过3个或者以上基站
        2. 基站的数量/记录数 < 1.5
        3. TODO 在道路上应该超过一定的时间
        4. TODO 车速应该满足一定条件
    """
    l = len(data)
    new_data = []
    continue_cnt = 0
    real_road_status = 0
    for i in range(0, l):
        i_row = data[i]
        if continue_cnt > 0:
            continue_cnt -= 1
            i_row.append(real_road_status)
            new_data.append(i_row)
            continue

        if i_row[road_index] != jm_load_id:
            # 如果不在jm大道上
            i_row.append(0)
            new_data.append(i_row)
            continue

        # 判断是否在jm大道上。基站已经在jm大道上
        stations = [i_row[station_index]]   # 记录经过的基站
        is_all_cross = i_row[cross_index] == 'N'     # 是否所有都是路口基站
        end_time = i_row[end_index]      # 结束时间
        j_continue_cnt = 0       # 处理时间有交叉的基站
        for j in range(i+1, l):
            if data[j][road_index] == jm_load_id:   # 如果基站在jm大道上
                if j_continue_cnt > 0 and end_time + 60 > data[j][start_index]:
                    # 中间有时间交叉的非jm大道上的基站记录
                    continue_cnt += j_continue_cnt
                    j_continue_cnt = 0
                elif j_continue_cnt > 0:
                    break

                continue_cnt += 1
                stations.append(data[j][station_index])
                end_time = data[j][end_index]
                if is_all_cross and data[j][cross_index] != 'N':
                    is_all_cross = False   # 只要包含非路口基站即可

            elif data[j][start_index] == data[j-1][start_index] \
                    and data[j][end_index] == data[j-1][end_index]:
                # 和前面记录的开始时间和结束时间完全一致，则可以跳过
                j_continue_cnt += 1
            elif data[j][start_index] < end_time:
                # 当前基站虽然不在jm大道上，但是当前的开始时间小于前一条记录的结束时间
                # 这种记录可能并不是正常的记录，可以忽略
                j_continue_cnt += 1
            else:
                break

        rate = float(continue_cnt+1) / float(len(set(stations)))
        if not is_all_cross and continue_cnt > 1 and rate < 1.45:
            # 连续3个及以上的基站
            i_row.append(jm_load_id)
            new_data.append(i_row)
            real_road_status = jm_load_id
            continue

        # 如果不满足在jd大道的条件
        i_row.append(0)
        new_data.append(i_row)
        real_road_status = 0

    nl = len(new_data)
    if l != nl:
        print("==> ERROR: user road map %d != %d" % (l, nl))
        print(data)
        print(new_data)

    return new_data


def merge_station(data):
    """合并相同的基站
    """
    l = len(data)
    new_data = []
    continue_cnt = 0
    for i in range(0, l):
        if continue_cnt > 0:
            continue_cnt -= 1
            continue

        # 判断是否在jm大道上
        i_row = data[i]
        last_end_time = i_row[end_index]
        interval_time = 0   # 每条记录前后的间隙的时间
        for j in range(i+1, l):
            if i_row[station_index] == data[j][station_index]:
                # 合并相同的基站
                i_row[end_index] = data[j][end_index]
                continue_cnt += 1
                if i_row[end_index] > last_end_time:
                    interval_time += i_row[end_index] - last_end_time
                    last_end_time = i_row[end_index]
            else:
                break

        # 消耗时间
        i_row.append(int((i_row[end_index] - i_row[start_index]) / 60))
        i_row.append(interval_time)
        new_data.append(i_row)

    nl = len(new_data)
    if l < nl:
        print("==> ERROR: merge_station %d < %d" % (l, nl))
        print(data)
        print(new_data)

    return new_data


def uniq_map(data):
    """去重：相同的开始时间，相同的基站，结束时间只保留最大的一个"""
    uniq_dict = {}
    for i in data:
        key = (i[start_index], i[station_index])
        if key not in uniq_dict:
            uniq_dict[key] = i
        elif i[end_index] > uniq_dict[key][end_index]:
            uniq_dict[key][end_index] = i[end_index]

    return [uniq_dict[i] for i in uniq_dict]


def load_map(row):
    """根据用户ID进行聚合数据
    实现的目标：
    1. 道路模式识别: 快速经过路上的若干个基站
    2. 路口模式识别: """
    row = list(row[1])  # 二维数组
    data = uniq_map(row)    # 去重数据
    data = sorted(data, key=cmp_to_key(cmp_time))  # 按开始时间和结束时间排序排序

    # TODO 临时过滤数据, 只保留13的数据
    data = [i for i in data if localtime(i[start_index]).tm_mday == 13]

    # 合并相同基站的数据
    l = len(data)
    data = merge_station(data)
    print("==> Total merge_station %d => %d" % (l, len(data)))

    # 道路模式识别
    data = user_road_map(data)

    # 路口模式识别
    data = user_cross_map(data)

    return data


def cmp_time(x, y):
    """开始时间和结束时间的比较"""
    if x[start_index] > y[start_index]:
        return 1
    elif x[start_index] < y[start_index]:
        return -1
    elif x[end_index] > y[end_index]:
        return 1
    elif x[end_index] < y[end_index]:
        return -1
    return 0


# 入口程序
if len(sys.argv) == 3:
    source_filename = sys.argv[1]   # 需要加载的csv文件
    target_path = sys.argv[2]       # 结果保存目录
    main()
else:
    print("+"*40)
    print("参数错误！")
    sys.exit(1)
