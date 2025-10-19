#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据处理模块

该模块负责处理和计算指数数据，包括：
1. 过滤缺失数据
2. 计算技术指标（移动平均线、布林带等）
3. 计算估值指标
"""

import logging
import pandas as pd
import numpy as np
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


def filter_consecutive_missing_data(df):
    """
    过滤掉从最早日期开始连续缺失pe_ttm.mcw、pb.mcw或dyr.mcw的记录
    
    Args:
        df (pandas.DataFrame): 包含指数数据的DataFrame
        
    Returns:
        pandas.DataFrame: 过滤后的DataFrame
    """
    # 检查是否有关键列
    key_columns = ['pe_ttm.mcw', 'pb.mcw', 'dyr.mcw']
    existing_columns = [col for col in key_columns if col in df.columns]
    
    # 如果没有关键列，直接返回原数据
    if not existing_columns:
        return df
    
    # 检查每一行是否至少有一个关键指标非空
    has_valid_data = df[existing_columns].notna().any(axis=1)
    
    # 找到第一个有有效数据的行索引
    first_valid_index = has_valid_data.idxmax() if has_valid_data.any() else len(df)
    
    # 返回从第一个有效数据行开始的所有数据
    return df.iloc[first_valid_index:].copy()


def calculate_technical_indicators(df):
    """
    计算技术指标
    
    Args:
        df (pandas.DataFrame): 包含指数数据的DataFrame
        
    Returns:
        pandas.DataFrame: 添加了技术指标的DataFrame
    """
    df = df.copy()
    
    # 计算移动平均线
    ma_periods = [5, 10, 20, 30, 60, 120, 250]
    for period in ma_periods:
        df[f'{period}日均线'] = df['收盘价'].rolling(window=period).mean()

    # 计算布林带
    bb_period = 20
    df['布林线中轨'] = df['收盘价'].rolling(window=bb_period).mean()
    bb_std = df['收盘价'].rolling(window=bb_period).std()
    df['布林线上轨'] = df['布林线中轨'] + 2 * bb_std
    df['布林线下轨'] = df['布林线中轨'] - 2 * bb_std

    # 计算收盘价在布林线中的位置
    df['布林线位置'] = (df['收盘价'] - df['布林线下轨']) / (df['布林线上轨'] - df['布林线下轨'])
    
    return df


def calculate_valuation_percentiles(df):
    """
    计算估值百分位
    
    Args:
        df (pandas.DataFrame): 包含指数数据的DataFrame
        
    Returns:
        pandas.DataFrame: 添加了估值百分位的DataFrame
    """
    df = df.copy()
    
    # 检查必要的列是否存在
    required_columns = ['市盈率', '市净率', '股息率']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise KeyError(f"缺少必要的列: {missing_columns}")
    
    # 计算市盈率百分位
    df['市盈率百分位'] = df['市盈率'].rolling(window=500, min_periods=1).apply(
        lambda x: x.rank(method='min', pct=True).iloc[-1]
    )
    
    # 计算市净率百分位
    df['市净率百分位'] = df['市净率'].rolling(window=500, min_periods=1).apply(
        lambda x: x.rank(method='min', pct=True).iloc[-1]
    )
    
    # 股息率需要反向处理，因为股息率越高表示估值越低
    # 为了与市盈率和市净率保持一致，需要1-排名百分位
    df['股息率收益率'] = df['股息率'].rolling(window=500, min_periods=1).apply(
        lambda x: 1 - x.rank(method='min', pct=True).iloc[-1]
    )

    # 估值百分位
    df['估值百分位'] = (df['市盈率百分位'] + df['市净率百分位'] + df['股息率收益率']) / 3
    
    return df


def process_index_data(index_info):
    """
    处理单个指数的数据
    
    Args:
        index_info (dict): 包含指数信息的字典
        
    Returns:
        dict: 更新后的指数信息
    """
    df = index_info["dataframe"]

    # 应用过滤函数去除开头连续缺失的数据
    df = filter_consecutive_missing_data(df)
    
    # 确保必要的列存在
    required_columns = ['收盘价', '市盈率', '市净率', '股息率']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise KeyError(f"缺少必要的列: {missing_columns}")
    
    # 计算技术指标
    df = calculate_technical_indicators(df)
    
    # 计算估值百分位
    df = calculate_valuation_percentiles(df)

    # 将计算后的数据更新到index_info中
    index_info["dataframe"] = df
    
    return index_info