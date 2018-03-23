from pyspark import SparkContext, SparkConf
from haversine import haversine
from itertools import groupby
import json
import time
import re
import sys


# 811078192773333,23,1492098117,1492098118,江门恩平小岛新村F-ZLH-2,112.31395,22.18322,恩平,恩城镇,否,宏站,室外,城区道路,\N,\N

USER_ID = 0                   # 用户 id
START_HOUR = 1                # 开始的小时数  int
START_TIME = 2                # 开始时间戳    int
END_TIME = 3                  # 结束时间戳    int
STATION = 4                   # 基站
LON = 5                       # 经纬度        float
LAT = 6                       # 经纬度        float
COUNTRY = 7                   # 区县
TOWN = 8                      # 镇
STATION_ROAD = 9              # 是/否 是否为江门大道上的基站
STATION_TYPE = 10             # 宏站
STATION_SIDE = 11             # 室外
COUNTRY_ROAD = 12             # 城区道路
# 13 \N
# 14 \N

# 从原始数据扩展出的字段
COST_TIME = 13                # 在一个地方停留的时间

SATTION_SUB = re.compile(r'([A-Z]-[A-Z]{3}-?\d*)')


def split_line(line):
    row = line.split(',')
    row[STATION] = SATTION_SUB.sub('', row[STATION])
    row[START_HOUR] = int(row[START_HOUR])
    row[START_TIME] = int(row[START_TIME])
    row[END_TIME] = int(row[END_TIME])
    row[LON] = float(row[LON])
    row[LAT] = float(row[LAT])
    row[13] = 0
    row[14] = 0
    return row


def find_route(rows, t=3):
    '''
    rows: 一个用户所有记录
    把用户相同基站记录合并
    return:一个用户所有记录
    '''
    rows = sorted(rows, key=lambda row: row[START_TIME])

    def merge_station(rows, t):
        route = []
        route.append(rows[0])
        route_station = [[], ]
        route_lat = [[], ]
        route_lon = [[], ]
        offset = 0
        for i in range(len(rows) - 1):
            if route[offset][STATION] != rows[i][STATION]:
                if route[offset][STATION] != rows[i + 1][STATION]:
                    route.append(rows[i])
                    route_station.append([])
                    route_lat.append([])
                    route_lon.append([])
                    offset += 1
            route_station[offset].append(rows[i][STATION])
            route_lat[offset].append(rows[i][LAT])
            route_lon[offset].append(rows[i][LON])
            route[offset][END_TIME] = rows[i][END_TIME]
        for i in range(len(route)):
            if len(route_station[i]) >= 2:
                route[i][STATION] = '-'.join(set(route_station[i]))
                route[i][LAT] = sum(route_lat[i]) / len(route_lat[i])
                route[i][LON] = sum(route_lon[i]) / len(route_lat[i])
        t = t - 1
        if t > 0:
            return merge_station(route, t)
        else:
            return route

    def time_count(route):
        for i in range(len(route)):
            route[i][COST_TIME] = route[i][END_TIME] - route[i][START_TIME]
        return route

    route = merge_station(rows, t)
    route = time_count(route)
    return route


def group_by_user(rows):
    '''
    rows: 一个用户所有记录
    return: 结构化数据
    '''
    def _init_key(row):
        info = {}
        info['station'] = row[STATION]
        info['town'] = row[TOWN]
        info['country'] = row[COUNTRY]
        info['station_road'] = row[STATION_ROAD]
        info['station_type'] = row[STATION_TYPE]
        info['station_side'] = row[STATION_SIDE]
        info['country_road'] = row[COUNTRY_ROAD]
        info['lon'] = row[LON]
        info['lat'] = row[LAT]
        info['status'] = []
        info['user_id'] = row[USER_ID]
        return info

    def _time_count(user):
        total_time = 0
        for p in user['points']:
            total_time += p['p_time']
        for p in user['points']:
            p['percent'] = p['p_time'] / total_time if total_time else 0
        return user
    rows = sorted(rows, key=lambda row: row[STATION])

    user = {}
    user['user_id'] = rows[0][USER_ID]
    user['points'] = []
    rows_grouped = groupby(rows, key=lambda row: row[STATION])
    for key, values in rows_grouped:
        values = sorted(values, key=lambda row: row[START_TIME])
        data = _init_key(values[0])
        data['status'] = []
        p_time = 0
        for row in values:
            diff_time = row[END_TIME] - row[START_TIME]
            start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row[START_TIME]))
            end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row[END_TIME]))
            line = {}
            line['start_hour'] = row[START_HOUR]
            line['start_time'] = start_time
            line['end_time'] = end_time
            line['cost_time'] = diff_time
            data['status'].append(line)
            p_time += diff_time
        data['p_time'] = p_time
        user['points'].append(data)
    user = _time_count(user)

    return user


def top_by_percent(user, top):
    user['points'] = sorted(user['points'], key=lambda p:p['percent'], reverse=True)[:top]
    return user


def analyze_user(user, percent=0.1, p_time=3600):
    def _analyze_by_status(status):
        '''
        通过在一个地方的逗留时间分析工作地还是居住地
        '''
        hours_info = [0 for i in range(24)]
        home = 0
        workplace = 0
        for s in status:
            diff = s['start_hour'] + s['cost_time'] // 3600
            for i in range(s['start_hour'], (diff + 1)):
                hours_info[i % 24] = 1
        count_per_day = 0
        for i in range(24):
            if hours_info[i] > 0:
                count_per_day += 1
                if count_per_day > 12:
                    return 'home'
            if (0 <= i <= 6) or (23 <= i <= 24):
                if hours_info[i] > 0:
                    home += 1
            elif (7 <= i <= 20):
                if hours_info[i] > 0:
                    workplace += 1
        if home >= 3:
            return 'home'
        if workplace >= 3:
            return 'workplace'
        return 'other'

    for p in user['points']:
        if p['percent'] > percent and p['p_time'] > p_time:
            p['place'] = _analyze_by_status(p['status'])
        else:
            p['place'] = 'other'
    return user


def dump_as_json(user):
    return json.dumps(user, ensure_ascii=False)


def task_home_workplace(sc, input_path):
    # 分析用户的居住地和工地距离
    def _filter_place(user):
        # 过滤出明确分析出居住职业地的用户
        home = False
        workplace = False
        for p in user['points']:
            if p['place'] == 'workplace':
                workplace = True
            elif p['place'] == 'home':
                home = True
        return home and workplace

    def _calc(user):
        # 计算两地的距离
        p1 = (user['points'][0]['lat'], user['points'][0]['lon'])
        p2 = (user['points'][1]['lat'], user['points'][1]['lon'])
        user['distance'] = haversine(p1, p2)
        return user

    rdd = sc.textFile(input_path)
    grdd = rdd.map(split_line).groupBy(lambda x:x[USER_ID])
    userdd = grdd.map(lambda x: find_route(x[1])).map(group_by_user)
    resultrdd = userdd.map(lambda user: top_by_percent(user, top=2))\
        .map(lambda user: analyze_user(user))\
        .filter(_filter_place)\
        .map(_calc)
    return resultrdd


def cli(input_path, output_path):
    conf = SparkConf().setAppName('JM City Home Workplace Analyse')
    sc = SparkContext(conf=conf)
    resultrdd = task_home_workplace(sc, input_path)
    resultrdd.map(dump_as_json).coalesce(4).saveAsTextFile(output_path)
    print('任务完成')
    sc.stop()


if __name__ == '__main__':
    if len(sys.argv) == 3:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        cli(input_path, output_path)
    else:
        print('参数错误')
