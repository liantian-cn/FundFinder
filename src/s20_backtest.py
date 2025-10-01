#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pickle
import pandas as pd
import pathlib
import logging
import numpy as np

from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def backtest_index(index_info):
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
        {'buy_threshold': 0.10, 'sell_threshold': 0.40, 'name': '策略.10-.40'},
        {'buy_threshold': 0.10, 'sell_threshold': 0.50, 'name': '策略.10-.50'},
        {'buy_threshold': 0.15, 'sell_threshold': 0.45, 'name': '策略.15-.45'},
        {'buy_threshold': 0.15, 'sell_threshold': 0.55, 'name': '策略.15-.55'},
        {'buy_threshold': 0.20, 'sell_threshold': 0.50, 'name': '策略.20-.50'},
        {'buy_threshold': 0.20, 'sell_threshold': 0.60, 'name': '策略.20-.60'},
        {'buy_threshold': 0.25, 'sell_threshold': 0.55, 'name': '策略.25-.55'},
        {'buy_threshold': 0.25, 'sell_threshold': 0.65, 'name': '策略.25-.65'},
        {'buy_threshold': 0.30, 'sell_threshold': 0.60, 'name': '策略.30-.60'},
        {'buy_threshold': 0.30, 'sell_threshold': 0.70, 'name': '策略.30-.70'},
        {'buy_threshold': 0.35, 'sell_threshold': 0.65, 'name': '策略.35-.65'},
        {'buy_threshold': 0.35, 'sell_threshold': 0.75, 'name': '策略.35-.75'},
        {'buy_threshold': 0.40, 'sell_threshold': 0.70, 'name': '策略.40-.70'},
        {'buy_threshold': 0.40, 'sell_threshold': 0.80, 'name': '策略.40-.80'},
        {'buy_threshold': 0.45, 'sell_threshold': 0.75, 'name': '策略.45-.75'},
        {'buy_threshold': 0.45, 'sell_threshold': 0.85, 'name': '策略.45-.85'},
        {'buy_threshold': 0.50, 'sell_threshold': 0.80, 'name': '策略.50-.80'},
        {'buy_threshold': 0.50, 'sell_threshold': 0.90, 'name': '策略.50-.90'},
        {'buy_threshold': 0.55, 'sell_threshold': 0.85, 'name': '策略.55-.85'},
        {'buy_threshold': 0.55, 'sell_threshold': 0.95, 'name': '策略.55-.95'}
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

    # 添加前一天的估值百分位用于判断上穿
    df_test['prev_估值百分位'] = df_test['估值百分位'].shift(1)

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

        # 遍历每个策略
        for strategy in strategies:
            # 买入条件：没有持仓，且上日益估值百分位低于阈值，且当日估值百分位高于等于阈值
            if (not strategy['position']) and (prev_pe_percentile < strategy['buy_threshold'] <= pe_percentile):
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
                stop_loss = current_return <= -0.10

                # 止盈条件：上一日低于阈值，当日高于等于阈值
                take_profit = (prev_pe_percentile < strategy['sell_threshold'] <= pe_percentile)

                # 如果触发止损或止盈
                if stop_loss or take_profit:
                    # 计算持有天数
                    holding_days = (date - strategy['position_date']).days

                    # 计算卖出后的资金
                    sell_capital = strategy['shares'] * next_open_price

                    # 计算本次收益
                    # 修复除零错误：检查 strategy['capital'] 是否为 0
                    if strategy['capital'] != 0:
                        trade_return = (sell_capital - strategy['capital']) / strategy['capital']
                    else:
                        trade_return = 0 if sell_capital == 0 else float('inf')
                        
                    strategy['total_return'] += trade_return

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
            # 使用最后一日的数据计算收益
            last_row = df_test.iloc[-1]
            last_price = last_row['收盘价']  # 使用收盘价作为最终价值
            final_capital = strategy['shares'] * last_price
            
            # 修复除零错误：检查 strategy['capital'] 是否为 0
            if strategy['capital'] != 0:
                final_return = (final_capital - strategy['capital']) / strategy['capital']
            else:
                final_return = 0 if final_capital == 0 else float('inf')

            # 计算持有天数
            holding_days = (last_row['日期'] - strategy['position_date']).days
            strategy['total_holding_days'] += holding_days
            strategy['total_return'] += final_return

            # 添加持仓结束日志
            position_end_log = {
                'date': last_row['日期'].strftime('%Y-%m-%d'),
                'strategy_name': strategy['name'],
                'direction': 'position_end',
                'amount': final_capital,
                'price': last_price,
                'cash': final_capital
            }
            log.append(position_end_log)
            # logging.info(f"持仓结束 - 日期:{position_end_log['日期']}, 策略:{position_end_log['策略名称']}, 价格:{position_end_log['价格']:.4f}")

    # 计算每个策略的最终年化收益率
    for strategy in strategies:
        strategy_stat = {
            'strategy_name': strategy['name'],
            'holding_days': strategy['total_holding_days'],
        }

        if strategy['total_holding_days'] > 0:
            total_return = strategy['total_return']
            strategy_stat['cumulative_return_percent'] = total_return * 100
            
            # 年化收益计算，避免出现复数
            base = 1 + strategy['total_return']
            if base > 0:  # 只有当base为正数时才计算幂
                annual_return = np.power(base, 365 / strategy['total_holding_days']) - 1
            else:
                annual_return = 0  # 如果base为负数或零，则年化收益设为0
                
            strategy_stat['total_return'] = total_return
            strategy_stat['annual_return'] = annual_return
        else:
            strategy_stat['cumulative_return_percent'] = 0
            strategy_stat['total_return'] = 0
            strategy_stat['annual_return'] = 0

        stat.append(strategy_stat)

    # 按年化收益从高到低排序
    stat.sort(key=lambda x: x['annual_return'], reverse=True)

    return log, stat


def backtest_strategy(data_path: pathlib.Path):
    """
    对指数进行回归测试
    """
    # 获取所有需要操作的指数
    filtered_index = json.load(open(data_path.joinpath("cn_index_filtered.json"), "r", encoding="utf-8"))

    index_dir = data_path.joinpath("index_info")

    # 遍历每个指数
    for index in filtered_index:
        logging.info(f"正在回测指数: {index['stockCode']} - {index['name']}")

        # 读取之前获取的数据
        with open(index_dir.joinpath(f"{index['stockCode']}.pickle"), "rb") as f:
            index_info = pickle.load(f)

        backtest_log,backtest_stat = backtest_index(index_info)
        logging.info(f"完成计算{index['stockCode']}的回测")

        index_info["backtest_log"] = backtest_log
        index_info["backtest_stat"] = backtest_stat
        # 保存更新后的数据
        with open(index_dir.joinpath(f"{index['stockCode']}.pickle"), "wb") as f:
            pickle.dump(index_info, f)

    logging.info("所有指数回测完成")