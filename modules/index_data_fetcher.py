#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数数据获取模块

该模块负责从API获取指数相关数据，包括：
1. 指数基本信息
2. 指数成分股信息
3. 跟踪指数的基金信息
"""

import json
import logging
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import retry, find_dict_by_field
from utils import query_json


@retry(max_attempts=5, delay=5)
def fetch_cn_index():
    """获取A股所有指数基础信息。
    
    Returns:
        list: 包含所有A股指数基础信息的列表
        
    Raises:
        Exception: 当API调用失败或返回非成功消息时抛出
    """
    logging.info("正在获取A股所有指数基础信息...")
    fetch = query_json("cn/index", {})
    if fetch['message'] != "success":
        logging.error(f"获取指数基础信息失败: {fetch.get('message', '未知错误')}")
        raise Exception("获取指数基础信息失败")
    index_data = fetch["data"]
    logging.info(f"成功获取 {len(index_data)} 条指数基础信息")
    return index_data


@retry(max_attempts=5, delay=5)
def fetch_cn_company():
    """获取A股所有公司基础信息。
    
    Returns:
        list: 包含所有A股公司基础信息的列表
        
    Raises:
        Exception: 当API调用失败或返回非成功消息时抛出
    """
    logging.info("正在获取A股所有公司基础信息...")
    fetch = query_json("cn/company", {"includeDelisted": True})
    if fetch['message'] != "success":
        logging.error(f"获取公司基础信息失败: {fetch.get('message', '未知错误')}")
        raise Exception("获取公司基础信息失败")
    company_info = fetch["data"]
    logging.info(f"成功获取 {len(company_info)} 条公司基础信息")
    return company_info


@retry(max_attempts=5, delay=5)
def fetch_index_constituent(stockCode, cn_company):
    """获取单个指数的成分股及其权重信息。
    
    Args:
        stockCode (str): 指数代码
        cn_company (list): 所有A股公司信息列表
        
    Returns:
        list: 按权重排序的成分股信息列表
        
    Raises:
        Exception: 当API调用失败或返回非成功消息时抛出
    """
    logging.debug(f"正在获取指数 {stockCode} 的成分股信息...")
    fetch = query_json("cn/index/constituent-weightings", {
        "startDate": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
        "endDate": datetime.now().strftime("%Y-%m-%d"),
        "stockCode": stockCode,
        "limit": 1000
    })
    if fetch['message'] != "success":
        logging.error(f"获取指数 {stockCode} 成分股信息失败: {fetch.get('message', '未知错误')}")
        raise Exception(f"获取指数 {stockCode} 成分股信息失败")
    
    constituent_weightings_dict = {}
    for item in fetch["data"]:
        # 检查item中是否存在stockCode键，避免KeyError
        if "stockCode" in item:
            if item["stockCode"] not in constituent_weightings_dict.keys():
                constituent_weightings_dict[item["stockCode"]] = item["weighting"]
        else:
            logging.warning(f"[{stockCode}]Index constituent item missing 'stockCode': {item}")
    
    constituent_weightings_list = []
    for key, value in constituent_weightings_dict.items():
        company = find_dict_by_field(cn_company, "stockCode", key)
        if company is not None:
            company["weighting"] = value
            constituent_weightings_list.append(company)
    
    constituent_weightings_list.sort(key=lambda x: x["weighting"], reverse=True)
    logging.debug(f"成功获取指数 {stockCode} 的 {len(constituent_weightings_list)} 条成分股信息")
    return constituent_weightings_list


@retry(max_attempts=5, delay=5)
def fetch_index_tracking_fund(stockCode):
    """获取跟踪特定指数的基金信息。
    
    Args:
        stockCode (str): 指数代码
        
    Returns:
        list: 跟踪该指数的基金信息列表
        
    Raises:
        Exception: 当API调用失败或返回非成功消息时抛出
    """
    logging.debug(f"正在获取跟踪指数 {stockCode} 的基金信息...")
    fetch = query_json("cn/index/tracking-fund", {
        "stockCode": stockCode,
    })
    if fetch['message'] != "success":
        logging.error(f"获取跟踪指数 {stockCode} 的基金信息失败: {fetch.get('message', '未知错误')}")
        raise Exception(f"获取跟踪指数 {stockCode} 的基金信息失败")
    
    fund_data = fetch["data"]
    logging.debug(f"成功获取跟踪指数 {stockCode} 的 {len(fund_data)} 条基金信息")
    return fund_data


def fetch_single_index_data(index, cn_company):
    """获取单个指数的完整信息，包括成分股和跟踪基金。
    
    Args:
        index (dict): 指数基础信息
        cn_company (list): 所有A股公司信息列表
        
    Returns:
        dict: 包含完整信息的指数数据
    """
    stockCode = index["stockCode"]
    logging.info(f"正在处理指数 {stockCode} - {index['name']}...")
    
    try:
        constituent_weightings = fetch_index_constituent(stockCode, cn_company)
        if len(constituent_weightings) > 30:
            constituent_weightings = constituent_weightings[:30]
        index["constituent_weightings"] = constituent_weightings
        
        index["tracking_fund"] = fetch_index_tracking_fund(stockCode)
        
        logging.info(f"成功处理指数 {stockCode} - {index['name']}")
        return index
    except Exception as e:
        logging.error(f"处理指数 {stockCode} - {index['name']} 时出错: {e}")
        raise


def update_index_info(cn_index_file, cn_company, max_workers=20):
    """更新所有指数的完整信息。
    
    Args:
        cn_index_file (pathlib.Path): 指数数据文件路径
        cn_company (list): 所有A股公司信息列表
        max_workers (int): 最大并发线程数
    """
    with open(cn_index_file, "r", encoding="utf-8") as f:
        cn_index = json.load(f)
    
    logging.info(f"开始更新 {len(cn_index)} 个指数的完整信息，最大并发数: {max_workers}")
    
    # 记录开始时间
    start_time = time.time()
    total_count = len(cn_index)
    completed_count = 0
    
    # 使用线程池并发执行
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_index = {
            executor.submit(fetch_single_index_data, index, cn_company): index 
            for index in cn_index
        }
        
        # 处理完成的任务
        results = []
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            try:
                result = future.result()
                results.append(result)
                completed_count += 1
                
                # 计算进度信息
                elapsed_time = time.time() - start_time
                avg_time_per_item = elapsed_time / completed_count if completed_count > 0 else 0
                
                # 打印进度信息
                logging.info(f"进度: {completed_count}/{total_count} ({completed_count/total_count*100:.1f}%) "
                            f"已完成 {index['stockCode']} - {index['name']}")
                
            except Exception as e:
                completed_count += 1
                logging.error(f"处理 {index['stockCode']} - {index['name']} 时出错: {e}")
    
    # 保存结果到文件
    with open(cn_index_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
    
    total_time = time.time() - start_time
    logging.info(f"完成更新所有指数信息，总共处理 {len(results)} 个指数，耗时 {total_time:.2f} 秒")