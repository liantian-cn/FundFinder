#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pickle
import pathlib
import time
import logging
import pytz
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np

from utils import retry, find_dict_by_field,get_dates_ranges
from utils import query_json

SHANGHAI_TZ = pytz.timezone("Asia/Shanghai")
BASE_DIR = pathlib.Path(__file__).parent
DATA_DIR = BASE_DIR.joinpath("data")
OUTPUT_DIR = BASE_DIR.joinpath("output")
if not DATA_DIR.exists():
    DATA_DIR.mkdir()





from typing import NoReturn
import pandas as pd


# 设置最大行数显示（None 表示无限制）
pd.set_option('display.max_rows', None)
# 设置最大列数显示（无限制）
pd.set_option('display.max_columns', None)
# 显示所有数据，不以省略号代替
pd.set_option('display.expand_frame_repr', False)
# 自动调整列宽以适应内容
pd.set_option('display.width', None)
# 最大宽度，避免列内容被截断
pd.set_option('display.max_colwidth', None)
# 关闭“紧凑”模式，确保换行和对齐更清晰
pd.set_option('display.precision', 6)  # 可选：浮点数精度

# 解决中文对齐问题的关键设置
# 禁用 Unicode 字符宽度折叠（pandas 有时误判中文字符宽度）
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)


def filter_consecutive_missing_data(df):
    """
    过滤掉从最早日期开始连续缺失pe_ttm.mcw、pb.mcw或dyr.mcw的记录
    
    Args:
        df (pandas.DataFrame): 包含指数数据的DataFrame
        
    Returns:
        pandas.DataFrame: 过滤后的DataFrame
    """
    # 找到第一个至少有一个关键指标非空的行索引
    key_columns = ['pe_ttm.mcw', 'pb.mcw', 'dyr.mcw']
    
    # 检查每一行是否至少有一个关键指标非空
    has_valid_data = df[key_columns].notna().any(axis=1)
    
    # 找到第一个有有效数据的行索引
    first_valid_index = has_valid_data.idxmax() if has_valid_data.any() else len(df)
    
    # 返回从第一个有效数据行开始的所有数据
    return df.iloc[first_valid_index:].copy()


@retry(max_attempts=5, delay=5)
def fetch_index_candlestick(index):
    end_datetime = datetime.now(SHANGHAI_TZ)
    launch_datetime = datetime.fromisoformat(index["launchDate"])

    result = []
    # 将日期分组
    date_ranges = get_dates_ranges(launch_datetime, end_datetime)

    for start, end in date_ranges:
        fetch = query_json(url_suffix="cn/index/candlestick",
                           query_params={
                               "stockCode": index["stockCode"],
                               "type": "normal",
                               "startDate": start,
                               "endDate": end,
                           })
        if fetch['message'] != "success":
            raise Exception
        result.extend(fetch["data"])

    for item in result:
        item["date"] = datetime.fromisoformat(item["date"]).strftime("%Y-%m-%d")

    df = pd.DataFrame(result)
    return df


@retry(max_attempts=5, delay=5)
def fetch_index_fundamental(index: dict):
    end_datetime = datetime.now(SHANGHAI_TZ)
    launch_datetime = datetime.fromisoformat(index["launchDate"])

    result = []
    date_ranges = get_dates_ranges(launch_datetime, end_datetime)

    for start, end in date_ranges:
        fetch = query_json(url_suffix="cn/index/fundamental",
                           query_params={
                               "stockCodes": [index["stockCode"], ],
                               "startDate": start,
                               "endDate": end,
                               "metricsList": [
                                   "pe_ttm.mcw",  # 滚动市盈率(市值加权)
                                   "pb.mcw",  # 市净率(市值加权)
                                   "dyr.mcw",  # 股息率(市值加权)
                               ]
                           })
        if fetch['message'] != "success":
            raise Exception
        result.extend(fetch["data"])

    for item in result:
        item["date"] = datetime.fromisoformat(item["date"]).strftime("%Y-%m-%d")

    df = pd.DataFrame(result)

    return df

def fetch_index(index):
    candlestick = fetch_index_candlestick(index)
    fundamental = fetch_index_fundamental(index)

    df = pd.merge(candlestick, fundamental, on='date', how='left')
    df = df.sort_values(by='date')
    df.reset_index(drop=True, inplace=True)
    
    # 应用过滤函数去除开头连续缺失的数据
    df = filter_consecutive_missing_data(df)

    df.rename(columns={
        'date': '日期',
        'volume': '成交量',
        'open': '开盘价',
        'high': '最高价',
        'low': '最低价',
        'close': '收盘价',
        'change': '涨跌幅',
        'amount': '成交额',
        'pe_ttm.mcw': '市盈率',
        'pb.mcw': '市净率',
        'dyr.mcw': '股息率',
        'stockCode': '股票代码'
    }, inplace=True)

    index["dataframe"] = df

    with open(DATA_DIR.joinpath(f"{index['stockCode']}.pickle"), "wb") as f:
        pickle.dump(index, f)

    return index


def fetch_data():
    cn_index = json.load(BASE_DIR.joinpath("cn_index_filtered.json").open(encoding="utf-8"))
    total_count = len(cn_index)
    completed_count = 0

    # 使用线程池并发执行，最大并发数20
    with ThreadPoolExecutor(max_workers=15) as executor:
        # 提交所有任务
        future_to_index = {
            executor.submit(fetch_index, index): index
            for index in cn_index
        }

        # 处理完成的任务
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                # 为每个任务设置30秒超时时间
                future.result(timeout=30)
                completed_count += 1

                # 打印进度信息
                logging.info(f"进度: {completed_count}/{total_count} ({completed_count/total_count*100:.1f}%) "
                             f"抓取指数信息 {index['stockCode']} - {index['name']}")

            except Exception as e:
                completed_count += 1
                logging.error(f"处理 {index['stockCode']} 时出错: {e}")


def calculate_index():
    cn_index = json.load(BASE_DIR.joinpath("cn_index_filtered.json").open(encoding="utf-8"))
    total_count = len(cn_index)
    completed_count = 0
    for index in cn_index:
        # 计算进度和时间信息
        completed_count += 1
        # 打印进度信息
        logging.info(f"进度: {completed_count}/{total_count} ({completed_count / total_count * 100:.1f}%) "
                     f"计算指数信息 {index['stockCode']} - {index['name']}")

        try:
            with open(DATA_DIR.joinpath(f"{index['stockCode']}.pickle"), "rb") as f:
                index_info = pickle.load(f)

            df = index_info["dataframe"]

            # 计算移动平均线
            ma_periods = [5, 10, 20, 30, 60, 120, 250]
            for period in ma_periods:
                df[f'{period}日均线'] = df['收盘价'].rolling(window=period).mean()

            bb_period = 20
            df['布林线中轨'] = df['收盘价'].rolling(window=bb_period).mean()
            bb_std = df['收盘价'].rolling(window=bb_period).std()
            df['布林线上轨'] = df['布林线中轨'] + 2 * bb_std
            df['布林线下轨'] = df['布林线中轨'] - 2 * bb_std

            # 计算收盘价在布林线中的位置
            df['布林线位置'] = (df['收盘价'] - df['布林线下轨']) / (df['布林线上轨'] - df['布林线下轨'])

            df['市盈率百分位'] = df['市盈率'].rolling(window=500, min_periods=1).apply(lambda x: x.rank(method='min', pct=True).iloc[-1])
            df['市净率百分位'] = df['市净率'].rolling(window=500, min_periods=1).apply(lambda x: x.rank(method='min', pct=True).iloc[-1])
            # 股息率需要反向处理，因为股息率越高表示估值越低，为了与市盈率和市净率保持一致，需要1-排名百分位
            df['股息率收益率'] = df['股息率'].rolling(window=500, min_periods=1).apply(lambda x: 1 - x.rank(method='min', pct=True).iloc[-1])

            # 估值百分位
            df['估值百分位'] = (df['市盈率百分位'] + df['市净率百分位'] + df['股息率收益率']) / 3

            # 将计算后的数据更新到index_info中
            index_info["dataframe"] = df

            # 保存更新后的数据
            with open(DATA_DIR.joinpath(f"{index['stockCode']}.pickle"), "wb") as f:
                pickle.dump(index_info, f)
        except Exception as e:
            logging.error(f"处理 {index['stockCode']} 时出错: {e}")


def backtest_single_index(index_info):
    df = index_info["dataframe"].copy()
    # 确保日期列是datetime类型
    df['日期'] = pd.to_datetime(df['日期'])

    # 设置回测开始时间：2016年1月1日之后，并且至少是第250个交易日
    start_date = datetime(2016, 1, 1)
    if len(df) <= 250:
        logging.info(f"  数据不足，跳过 {index_info['stockCode']}")
        return [], []

    # 找到2016年1月1日之后的数据
    df_filtered = df[df['日期'] >= start_date]
    if len(df_filtered) <= 250:
        logging.info(f"  2016年后数据不足250个交易日，跳过 {index_info['stockCode']}")
        return [], []

    # 从第250个交易日开始回测
    df_test = df_filtered.iloc[250:].reset_index(drop=True)

    if len(df_test) == 0:
        logging.info(f"  回测数据为空，跳过 {index_info['stockCode']}")
        return [], []

    # 初始化5个份额的策略参数
    strategies = [
        {'buy_threshold': 0.10, 'sell_threshold': 0.40, 'name': '10-40估值线', "mode": 1},
        {'buy_threshold': 0.10, 'sell_threshold': 0.50, 'name': '10-50估值线', "mode": 1},
        {'buy_threshold': 0.15, 'sell_threshold': 0.45, 'name': '15-45估值线', "mode": 1},
        {'buy_threshold': 0.15, 'sell_threshold': 0.55, 'name': '15-55估值线', "mode": 1},
        {'buy_threshold': 0.20, 'sell_threshold': 0.50, 'name': '20-50估值线', "mode": 1},
        {'buy_threshold': 0.20, 'sell_threshold': 0.60, 'name': '20-60估值线', "mode": 1},
        {'buy_threshold': 0.25, 'sell_threshold': 0.55, 'name': '25-55估值线', "mode": 1},
        {'buy_threshold': 0.25, 'sell_threshold': 0.65, 'name': '25-65估值线', "mode": 1},
        {'buy_threshold': 0.30, 'sell_threshold': 0.60, 'name': '30-60估值线', "mode": 1},
        {'buy_threshold': 0.30, 'sell_threshold': 0.70, 'name': '30-70估值线', "mode": 1},
        {'buy_threshold': 0.35, 'sell_threshold': 0.65, 'name': '35-65估值线', "mode": 1},
        {'buy_threshold': 0.35, 'sell_threshold': 0.75, 'name': '35-75估值线', "mode": 1},
        {'buy_threshold': 0.40, 'sell_threshold': 0.70, 'name': '40-70估值线', "mode": 1},
        {'buy_threshold': 0.40, 'sell_threshold': 0.80, 'name': '40-80估值线', "mode": 1},
        {'buy_threshold': 0.45, 'sell_threshold': 0.75, 'name': '45-75估值线', "mode": 1},
        {'buy_threshold': 0.45, 'sell_threshold': 0.85, 'name': '45-85估值线', "mode": 1},
        {'buy_threshold': 0.50, 'sell_threshold': 0.80, 'name': '50-80估值线', "mode": 1},
        {'buy_threshold': 0.50, 'sell_threshold': 0.90, 'name': '50-90估值线', "mode": 1},
        {'buy_threshold': 0.55, 'sell_threshold': 0.85, 'name': '55-85估值线', "mode": 1},
        {'buy_threshold': 0.55, 'sell_threshold': 0.95, 'name': '55-95估值线', "mode": 1},

        {'buy_threshold': 0.10, 'sell_threshold': 0.40, 'name': '10-40布林线', "mode": 2},
        {'buy_threshold': 0.10, 'sell_threshold': 0.50, 'name': '10-50布林线', "mode": 2},
        {'buy_threshold': 0.15, 'sell_threshold': 0.45, 'name': '15-45布林线', "mode": 2},
        {'buy_threshold': 0.15, 'sell_threshold': 0.55, 'name': '15-55布林线', "mode": 2},
        {'buy_threshold': 0.20, 'sell_threshold': 0.50, 'name': '20-50布林线', "mode": 2},
        {'buy_threshold': 0.20, 'sell_threshold': 0.60, 'name': '20-60布林线', "mode": 2},
        {'buy_threshold': 0.25, 'sell_threshold': 0.55, 'name': '25-55布林线', "mode": 2},
        {'buy_threshold': 0.25, 'sell_threshold': 0.65, 'name': '25-65布林线', "mode": 2},
        {'buy_threshold': 0.30, 'sell_threshold': 0.60, 'name': '30-60布林线', "mode": 2},
        {'buy_threshold': 0.30, 'sell_threshold': 0.70, 'name': '30-70布林线', "mode": 2},
        {'buy_threshold': 0.35, 'sell_threshold': 0.65, 'name': '35-65布林线', "mode": 2},
        {'buy_threshold': 0.35, 'sell_threshold': 0.75, 'name': '35-75布林线', "mode": 2},
        {'buy_threshold': 0.40, 'sell_threshold': 0.70, 'name': '40-70布林线', "mode": 2},
        {'buy_threshold': 0.40, 'sell_threshold': 0.80, 'name': '40-80布林线', "mode": 2},
        {'buy_threshold': 0.45, 'sell_threshold': 0.75, 'name': '45-75布林线', "mode": 2},
        {'buy_threshold': 0.45, 'sell_threshold': 0.85, 'name': '45-85布林线', "mode": 2},
        {'buy_threshold': 0.50, 'sell_threshold': 0.80, 'name': '50-80布林线', "mode": 2},
        {'buy_threshold': 0.50, 'sell_threshold': 0.90, 'name': '50-90布林线', "mode": 2},
        {'buy_threshold': 0.55, 'sell_threshold': 0.85, 'name': '55-85布林线', "mode": 2},
        {'buy_threshold': 0.55, 'sell_threshold': 0.95, 'name': '55-95布林线', "mode": 2},
    ]

    # 为每个策略初始化状态
    for strategy in strategies:
        strategy['position'] = False  # 是否持仓
        strategy['position_date'] = None  # 持仓开始日期
        strategy['buy_price'] = 0  # 买入价格
        strategy['capital'] = 100000  # 每份资金10万元
        strategy['shares'] = 0  # 持有份额
        strategy['total_holding_days'] = 0  # 总持仓天数
        strategy['total_return'] = 0  # 累计收益

    # 添加前一天的估值百分位和布林线位置用于判断上穿
    df_test['prev_估值百分位'] = df_test['估值百分位'].shift(1)
    df_test['prev_布林线位置'] = df_test['布林线位置'].shift(1)

    # 添加下一天的开盘价用于交易执行
    df_test['next_开盘价'] = df_test['开盘价'].shift(-1)

    # 初始化日志列表
    log = []
    stat = []

    # 开始回测
    for i, row in df_test.iterrows():
        if pd.isna(row['prev_估值百分位']) or pd.isna(row['next_开盘价']):
            continue

        date = row['日期']
        next_open_price = row['next_开盘价']  # 下一日开盘价用于交易
        pe_percentile = row['估值百分位']
        prev_pe_percentile = row['prev_估值百分位']  # 前一日估值百分位
        bollinger_position = row['布林线位置']
        prev_bollinger_position = row['prev_布林线位置']

        # 遍历每个策略
        for strategy in strategies:
            # 根据策略模式选择不同的买卖信号判断方法
            if strategy['mode'] == 1:
                # 模式1：使用估值百分位作为买卖信号
                buy_signal = (not strategy['position']) and (prev_pe_percentile < strategy['buy_threshold'] <= pe_percentile)
                sell_signal = strategy['position'] and strategy['capital'] > 0 and (prev_pe_percentile < strategy['sell_threshold'] <= pe_percentile)
            elif strategy['mode'] == 2:
                # 模式2：使用布林线位置作为买卖信号
                buy_signal = (not strategy['position']) and (prev_bollinger_position < strategy['buy_threshold'] <= bollinger_position)
                sell_signal = strategy['position'] and strategy['capital'] > 0 and (prev_bollinger_position < strategy['sell_threshold'] <= bollinger_position)

            # 买入条件
            if buy_signal:
                strategy['position'] = True
                strategy['position_date'] = date
                strategy['buy_price'] = next_open_price
                strategy['shares'] = strategy['capital'] / next_open_price
                # 添加买入日志
                buy_log = {
                    'date': date.strftime('%Y-%m-%d'),
                    'strategy_name': strategy['name'],
                    'direction': 'buy',
                    'amount': strategy['capital'],
                    'price': next_open_price,
                    'cash': strategy['capital']
                }
                log.append(buy_log)
                # logging.info(f"买入 - 日期:{buy_log['日期']}, 策略:{buy_log['策略名称']}, 价格:{buy_log['价格']:.4f}")

            # 检查是否需要卖出（止盈或止损）
            elif strategy['position'] and strategy['capital'] > 0:
                # 计算当前收益率
                current_return = (next_open_price - strategy['buy_price']) / strategy['buy_price'] if strategy['buy_price'] != 0 else 0

                # 止损条件：下跌超过15%
                stop_loss = current_return <= -0.15

                # 止盈条件：根据策略模式决定
                if strategy['mode'] == 1:
                    # 模式1：估值百分位上穿卖出阈值
                    take_profit = (prev_pe_percentile < strategy['sell_threshold'] <= pe_percentile)
                elif strategy['mode'] == 2:
                    # 模式2：布林线位置上穿卖出阈值
                    take_profit = (prev_bollinger_position < strategy['sell_threshold'] <= bollinger_position)

                # 如果触发止损或止盈
                if stop_loss or take_profit:
                    # 计算持有天数
                    holding_days = (date - strategy['position_date']).days

                    # 计算卖出后的资金
                    sell_capital = strategy['shares'] * next_open_price

                    # 累计总持仓天数
                    strategy['total_holding_days'] += holding_days

                    sell_type = "stop_loss" if stop_loss else "take_profit"
                    # 添加卖出日志
                    sell_log = {
                        'date': date.strftime('%Y-%m-%d'),
                        'strategy_name': strategy['name'],
                        'direction': sell_type + '_sell',
                        'amount': sell_capital,
                        'price': next_open_price,
                        'cash': sell_capital
                    }
                    log.append(sell_log)
                    # logging.info(f"{sell_type}卖出 - 日期:{sell_log['日期']}, 策略:{sell_log['策略名称']}, 价格:{sell_log['价格']:.4f}, 收益率:{trade_return*100:.2f}%")

                    # 重置状态
                    strategy['position'] = False
                    strategy['position_date'] = None
                    strategy['buy_price'] = 0
                    strategy['shares'] = 0
                    strategy['capital'] = sell_capital

    # 处理仍持仓的策略（已买入但从未卖出的情况）
    for strategy in strategies:
        if strategy['position']:
            # 使用最后一日的下一日开盘价强制卖出
            last_row = df_test.iloc[-1]
            sell_price = last_row['next_开盘价']  # 使用下一日开盘价作为卖出价格

            # 如果没有下一日开盘价，则使用当日收盘价
            if pd.isna(sell_price):
                sell_price = last_row['收盘价']

            # 计算卖出后的资金
            sell_capital = strategy['shares'] * sell_price

            # 计算持有天数
            holding_days = (last_row['日期'] - strategy['position_date']).days

            strategy['total_holding_days'] += holding_days

            # 添加强制卖出日志
            force_sell_log = {
                'date': last_row['日期'].strftime('%Y-%m-%d'),
                'strategy_name': strategy['name'],
                'direction': 'force_sell',
                'amount': sell_capital,
                'price': sell_price,
                'cash': sell_capital
            }
            log.append(force_sell_log)

            # 重置状态
            strategy['position'] = False
            strategy['position_date'] = None
            strategy['buy_price'] = 0
            strategy['shares'] = 0
            strategy['capital'] = sell_capital

    # 计算每个策略的最终年化收益率
    for strategy in strategies:
        # 计算总收益：最终资本减去本金（100000）
        total_return = strategy['capital'] - 100000

        # 计算年化收益率
        annual_return = 0
        if strategy['total_holding_days'] > 0:
            # 总收益率 = 总收益 / 本金
            total_rate = total_return / 100000
            # 年化收益率 = (1 + 总收益率) ^ (365 / 持仓天数) - 1
            base = 1 + total_rate
            if base > 0:  # 只有当base为正数时才计算幂
                annual_return = np.power(base, 365 / strategy['total_holding_days']) - 1
            else:
                annual_return = 0  # 如果base为负数或零，则年化收益设为0

        strategy_stat = {
            'strategy_name': strategy['name'],
            'holding_days': strategy['total_holding_days'],
            'capital': strategy['capital'],
            'total_return': total_return,
            'annual_return': annual_return
        }

        stat.append(strategy_stat)

    # 按年化收益从高到低排序
    stat.sort(key=lambda x: x['annual_return'], reverse=True)

    return log, stat


def backtest_index():
    cn_index = json.load(BASE_DIR.joinpath("cn_index_filtered.json").open(encoding="utf-8"))
    total_count = len(cn_index)
    completed_count = 0
    for index in cn_index:

        # 计算进度和时间信息
        completed_count += 1
        # 打印进度信息
        logging.info(f"进度: {completed_count}/{total_count} ({completed_count / total_count * 100:.1f}%) "
                     f"回测指数信息 {index['stockCode']} - {index['name']}")

        try:
            with open(DATA_DIR.joinpath(f"{index['stockCode']}.pickle"), "rb") as f:
                index_info = pickle.load(f)
            backtest_log, backtest_stat = backtest_single_index(index_info)
            index_info["backtest_log"] = backtest_log
            index_info["backtest_stat"] = backtest_stat

            # 保存更新后的数据
            with open(DATA_DIR.joinpath(f"{index['stockCode']}.pickle"), "wb") as f:
                pickle.dump(index_info, f)
        except Exception as e:
            logging.error(f"回测处理 {index['stockCode']} 时出错: {e}")



def export_to_js():
    cn_index = json.load(BASE_DIR.joinpath("cn_index_filtered.json").open(encoding="utf-8"))

    # 定义中文列名到英文列名的映射
    column_mapping = {
        '日期': 'date',
        '开盘价': 'open',
        '收盘价': 'close',
        '最低价': 'low',
        '最高价': 'high',
        '成交量': 'volume',
        '涨跌幅': 'change',
        '成交额': 'amount',
        '市盈率': 'pe_ttm',
        '市净率': 'pb',
        '股息率': 'dyr',
        '股票代码': 'stockCode',
        '5日均线': 'ma5',
        '10日均线': 'ma10',
        '20日均线': 'ma20',
        '30日均线': 'ma30',
        '60日均线': 'ma60',
        '120日均线': 'ma120',
        '250日均线': 'ma250',
        '布林线中轨': 'bb_middle',
        '布林线上轨': 'bb_upper',
        '布林线下轨': 'bb_lower',
        '布林线位置': 'bb_position',
        '市盈率百分位': 'pe_percentile',
        '市净率百分位': 'pb_percentile',
        '股息率收益率': 'dyr_percentile',
        '估值百分位': 'valuation_percentile'
    }

    # index_dir = data_path.joinpath("index_info")
    # output_path = output_path.joinpath("index")
    OUTPUT_INDEX_DIR = OUTPUT_DIR.joinpath("index")
    if not OUTPUT_INDEX_DIR.exists():
        OUTPUT_INDEX_DIR.mkdir()

    total_count = len(cn_index)
    completed_count = 0
    for index in cn_index:
        completed_count += 1
        logging.info(f"进度: {completed_count}/{total_count} ({completed_count / total_count * 100:.1f}%) "
                     f"导出到js指数信息 {index['stockCode']} - {index['name']}")
        with open(DATA_DIR.joinpath(f"{index['stockCode']}.pickle"), "rb") as f:
            index_info = pickle.load(f)

        # 创建一个副本以避免修改原始数据
        df = index_info["dataframe"].copy()

        # 重命名列名为英文
        df.rename(columns=column_mapping, inplace=True)

        # 导出为JSON格式
        index_info["dataframe"] = json.loads(df.to_json(orient="records", indent=4))
        with open(OUTPUT_INDEX_DIR.joinpath(f"{index['stockCode']}.json"), "w", encoding="utf-8") as f:
            json.dump(index_info, f, ensure_ascii=False, indent=4)
    logging.info("所有指数导出完成")


def export_home():
    cn_index = json.load(BASE_DIR.joinpath("cn_index_filtered.json").open(encoding="utf-8"))

    OUTPUT_INDEX_DIR = OUTPUT_DIR.joinpath("index")

    result = []

    for index in cn_index:
        with open(DATA_DIR.joinpath(f"{index['stockCode']}.pickle"), "rb") as f:
            index_info = pickle.load(f)

        df = index_info["dataframe"]
        entry = df.to_dict('records')[-1]

        entry["tracking_fund_count"] = len(index_info["tracking_fund"])
        entry["name"] = index_info["name"]

        backtest_stat = index_info["backtest_stat"]

        total_holding_days = sum(stat["holding_days"] for stat in backtest_stat)

        backtest_avg = 0
        for stat in backtest_stat:
            # 统计权重，来自持仓时间
            stat_weight = stat["holding_days"] / total_holding_days
            backtest_avg += stat_weight * stat["annual_return"]

        entry["backtest_avg"] = backtest_avg
        # print(entry)

        result.append(entry)

    # 处理NaN值，避免JSON序列化错误
    for item in result:
        for key, value in item.items():
            if isinstance(value, float) and np.isnan(value):
                item[key] = None

    with open(OUTPUT_INDEX_DIR.joinpath("home.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

def main():
    fetch_data()
    calculate_index()
    backtest_index()
    export_to_js()
    export_home()

if __name__ == '__main__':
    main()
