#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pathlib
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import retry, find_dict_by_field
from utils import query_json


BASE_DIR = pathlib.Path(__file__).parent

config = json.load(open(BASE_DIR.joinpath("config.json"), encoding="utf-8"))


@retry(max_attempts=5, delay=5)
def fetch_cn_index():
    """获取A股所有指数基础信息。"""
    fetch = query_json("cn/index", {})
    if fetch['message'] != "success":
        raise Exception
    index_data = fetch["data"]
    return index_data




@retry(max_attempts=5, delay=5)
def fetch_cn_company():
    """获取A股所有公司基础信息。"""
    file_path = pathlib.Path(__file__).parent.joinpath("cn_company.json")
    fetch = query_json("cn/company", {"includeDelisted":True})
    if fetch['message'] != "success":
        raise Exception
    company_info = fetch["data"]
    return company_info

@retry(max_attempts=5, delay=5)
def fetch_index_constituent(stockCode,cn_company):
    """获取单个指数的信息"""
    fetch = query_json("cn/index/constituent-weightings", {
        "startDate": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
        "endDate": datetime.now().strftime("%Y-%m-%d"),
        "stockCode": stockCode,
        "limit": 1000
    })
    if fetch['message'] != "success":
        raise Exception
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
    return constituent_weightings_list


@retry(max_attempts=5, delay=5)
def fetch_index_tracking_fund(stockCode):
    fetch = query_json("cn/index/tracking-fund", {
        "stockCode": stockCode,
    })
    if fetch['message'] != "success":
        raise Exception
    return fetch["data"]


def fetch_single_index_data(index, cn_company):
    stockCode = index["stockCode"]
    constituent_weightings = fetch_index_constituent(stockCode, cn_company)
    if len(constituent_weightings) > 30:
        constituent_weightings = constituent_weightings[:30]
    index["constituent_weightings"] = constituent_weightings
    index["tracking_fund"] = fetch_index_tracking_fund(stockCode)
    return index

def update_index_info(cn_index_file,cn_company):
    cn_index = json.load(open(BASE_DIR.joinpath("cn_index.json"), encoding="utf-8"))
    
    # 记录开始时间
    start_time = time.time()
    total_count = len(cn_index)
    completed_count = 0
    
    # 使用线程池并发执行，最大并发数20
    with ThreadPoolExecutor(max_workers=20) as executor:
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
                remaining_items = total_count - completed_count
                estimated_remaining_time = avg_time_per_item * remaining_items
                
                # 打印进度信息
                logging.info(f"进度: {completed_count}/{total_count} ({completed_count/total_count*100:.1f}%) "
                            f"已完成 {index['stockCode']} - {index['name']}")
                
            except Exception as e:
                completed_count += 1
                logging.error(f"处理 {index['stockCode']} 时出错: {e}")
    
    # 保存结果到文件
    with open(cn_index_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)


def main():
    cn_index = fetch_cn_index()
    cn_index_file = pathlib.Path(__file__).parent.joinpath("cn_index.json")
    with open(cn_index_file, "w",encoding="utf-8") as f:
        json.dump(cn_index, f, indent=4, ensure_ascii=False)

    cn_company = fetch_cn_company()
    cn_company_file = pathlib.Path(__file__).parent.joinpath("cn_company.json")
    with open(cn_company_file, "w",encoding="utf-8") as f:
        json.dump(cn_company, f, indent=4, ensure_ascii=False)

    update_index_info(cn_index_file,cn_company)

    return
if __name__ == '__main__':
    main()