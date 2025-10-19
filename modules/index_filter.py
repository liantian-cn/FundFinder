#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数筛选模块

该模块负责根据指定条件筛选指数数据。
"""

import logging
from datetime import datetime
import pytz

# 获取当前时间
NOW = datetime.now(pytz.timezone("Asia/Shanghai"))


def filter_indices_by_criteria(cn_index, min_years=3):
    """根据筛选条件过滤指数数据。
    
    筛选条件：
    1. 指数成立时间需满指定年数（默认3年）
    2. 指数必须有成分股信息
    3. 指数必须有跟踪基金
    
    Args:
        cn_index (list): 包含所有指数数据的列表
        min_years (int): 指数成立的最小年数要求，默认为3年
        
    Returns:
        list: 筛选后的指数数据列表
    """
    logging.info(f"开始筛选 {len(cn_index)} 个指数，最低成立年限: {min_years}年")
    
    for index in cn_index:
        index["enable"] = True
        # 检查指数成立时间是否满足要求
        launch_date = datetime.fromisoformat(index["launchDate"])
        if (NOW - launch_date).days < (365 * min_years):
            index["enable"] = False
            continue

        # 检查是否有成分股信息
        if len(index["constituent_weightings"]) == 0:
            index["enable"] = False
            continue

        # 检查是否有跟踪基金
        if len(index["tracking_fund"]) == 0:
            index["enable"] = False
            continue

    # 过滤掉不满足条件的指数
    filtered_indices = [item for item in cn_index if item["enable"]]
    
    logging.info(f"筛选完成，剩余 {len(filtered_indices)} 个指数")
    return filtered_indices