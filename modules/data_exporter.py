#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据导出模块

该模块负责将处理后的数据导出为JSON格式，供前端使用。
"""

import json
import logging
import pandas as pd
import numpy as np
import pickle
from pathlib import Path


def mean_with_default(arr, default_value=0):
    """
    计算数组均值，如果数组为空则返回默认值
    
    Args:
        arr: 输入数组
        default_value: 默认值
        
    Returns:
        数组均值或默认值
    """
    if len(arr) == 0:
        return default_value
    mean_value = np.mean(arr)
    return mean_value if not np.isnan(mean_value) else default_value


def export_index_to_js(index_info, output_dir):
    """
    将单个指数数据导出为JS格式
    
    Args:
        index_info (dict): 包含指数信息的字典
        output_dir (Path): 输出目录路径
    """
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

    # 创建一个副本以避免修改原始数据
    df = index_info["dataframe"].copy()

    # 重命名列名为英文
    df.rename(columns=column_mapping, inplace=True)

    # 导出为JSON格式
    index_info["dataframe"] = json.loads(df.to_json(orient="records", indent=4))
    
    with open(output_dir.joinpath(f"{index_info['stockCode']}.json"), "w", encoding="utf-8") as f:
        json.dump(index_info, f, ensure_ascii=False, indent=4)


def export_home_data(index_list, data_dir, output_dir):
    """
    导出首页数据
    
    Args:
        index_list (list): 指数列表
        data_dir (Path): 数据目录路径
        output_dir (Path): 输出目录路径
    """
    result = []

    for index in index_list:
        with open(data_dir.joinpath(f"{index['stockCode']}.pickle"), "rb") as f:
            index_info = pickle.load(f)

        df = index_info["dataframe"]
        entry = df.to_dict('records')[-1]

        entry["tracking_fund_count"] = len(index_info.get("tracking_fund", []))
        entry["name"] = index_info["name"]

        # 安全地获取回测统计数据，如果不存在则使用空列表
        backtest_stat = index_info.get("backtest_stat", [])

        # 分类策略状态
        fundamental_stat  = [stat for stat in backtest_stat if stat["mode"] == "fundamental"]
        bollinger_stat = [stat for stat in backtest_stat if stat["mode"] == "bollinger"]

        # 求2种估值的年化收益中位数，去掉持仓小于15%的。
        fundamental_rate = [stat["strategy_duration_rate"] for stat in fundamental_stat if stat["position_rate"] > 0.15]
        bollinger_rate = [stat["strategy_duration_rate"] for stat in bollinger_stat if stat["position_rate"] > 0.15]
        fundamental_rate_median = mean_with_default(fundamental_rate)
        bollinger_rate_median = mean_with_default(bollinger_rate)

        # 过滤出收益大于中位数的策略
        high_fundamental_stat = [stat for stat in fundamental_stat if stat["strategy_duration_rate"] > fundamental_rate_median]
        high_bollinger_stat = [stat for stat in bollinger_stat if stat["strategy_duration_rate"] > bollinger_rate_median]

        # 求这些策略的平均买入、卖出价格
        high_fundamental_buy_price = mean_with_default([stat["buy_threshold"] for stat in high_fundamental_stat])
        high_fundamental_sell_price = mean_with_default([stat["sell_threshold"] for stat in high_fundamental_stat])
        high_bollinger_buy_price = mean_with_default([stat["buy_threshold"] for stat in high_bollinger_stat])
        high_bollinger_sell_price = mean_with_default([stat["sell_threshold"] for stat in high_bollinger_stat])

        # 平均收益率
        high_fundamental_rate_mean = mean_with_default([stat["strategy_duration_rate"] for stat in high_fundamental_stat])
        high_bollinger_rate_mean = mean_with_default([stat["strategy_duration_rate"] for stat in high_bollinger_stat])

        entry["fundamental_rate"] = high_fundamental_rate_mean
        entry["bollinger_rate"] = high_bollinger_rate_mean
        entry["fundamental_buy_price"] = high_fundamental_buy_price
        entry["fundamental_sell_price"] = high_fundamental_sell_price
        entry["bollinger_buy_price"] = high_bollinger_buy_price
        entry["bollinger_sell_price"] = high_bollinger_sell_price

        result.append(entry)

    # 处理NaN值，避免JSON序列化错误
    for item in result:
        for key, value in item.items():
            if isinstance(value, float) and np.isnan(value):
                item[key] = None

    with open(output_dir.joinpath("home.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)