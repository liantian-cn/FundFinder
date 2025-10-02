import json
import pickle
import logging
import pytz
import pathlib
from datetime import datetime, timedelta
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import retry, find_dict_by_field, get_dates_ranges
from utils import query_json

SHANGHAI_TZ = pytz.timezone("Asia/Shanghai")


@retry(max_attempts=5, delay=5)
def fetch_index_constituent_weightings(index: dict, company_info: list):
    fetch = query_json("cn/index/constituent-weightings", {
        "startDate": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
        "endDate": datetime.now().strftime("%Y-%m-%d"),
        "stockCode": index["stockCode"],
        "limit": 1000
    })
    if fetch['message'] != "success":
        raise Exception
    constituent_weightings_dict = {}
    for item in fetch["data"]:
        if item["stockCode"] not in constituent_weightings_dict.keys():
            constituent_weightings_dict[item["stockCode"]] = item["weighting"]
    # print(constituent_weightings_dict)
    # print(fetch)
    constituent_weightings_list = []
    for key, value in constituent_weightings_dict.items():
        company = find_dict_by_field(company_info, "stockCode", key)
        if company is not None:
            company["weighting"] = value
            constituent_weightings_list.append(company)

    constituent_weightings_list.sort(key=lambda x: x["weighting"], reverse=True)
    return constituent_weightings_list


@retry(max_attempts=5, delay=5)
def fetch_index_tracking_fund(index: dict):
    fetch = query_json("cn/index/tracking-fund", {
        "stockCode": index["stockCode"],
    })
    if fetch['message'] != "success":
        raise Exception
    return fetch["data"]


@retry(max_attempts=5, delay=5)
def fetch_index_candlestick(index: dict):
    end_datetime = datetime.now(SHANGHAI_TZ)
    launch_datetime = datetime.fromisoformat(index["launchDate"])

    result = []
    # 将日期分组
    date_ranges = get_dates_ranges(launch_datetime, end_datetime)

    for start, end in date_ranges:
        # logging.info(f"正在抓取{index['stockCode']}的K线数据，{start} - {end}")
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
    # 将日期分组
    date_ranges = get_dates_ranges(launch_datetime, end_datetime)

    for start, end in date_ranges:
        # logging.info(f"正在抓取{index['stockCode']}的基本面数据，{start} - {end}")
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
            print(fetch)
            raise Exception
        result.extend(fetch["data"])

    for item in result:
        item["date"] = datetime.fromisoformat(item["date"]).strftime("%Y-%m-%d")

    df = pd.DataFrame(result)

    return df


def fetch_single_index_data(index, company_info):
    """抓取单个指数的所有数据"""

    # 抓取权重信息
    # logging.info(f"正在抓取{index['stockCode']}的权重信息")
    constituent_weightings = fetch_index_constituent_weightings(index, company_info)
    if len(constituent_weightings) > 30:
        constituent_weightings = constituent_weightings[:30]
    index["constituent_weightings"] = constituent_weightings

    # 抓取跟踪基金
    # logging.info(f"正在抓取{index['stockCode']}的跟踪基金")
    index["tracking_fund"] = fetch_index_tracking_fund(index)

    # 抓取K线数据
    # logging.info(f"正在抓取{index['stockCode']}的K线数据")
    candlestick = fetch_index_candlestick(index)

    # 抓取基本面数据
    # logging.info(f"正在抓取{index['stockCode']}的基本面数据")
    fundamental = fetch_index_fundamental(index)

    # 将K线和基本面数据合并到history中
    df = pd.merge(candlestick, fundamental, on='date', how='left')
    df = df.sort_values(by='date')
    df.reset_index(drop=True, inplace=True)

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
    index["update"] = datetime.now(SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M:%S")

    return index



def fetch_index_info(data_path: pathlib.Path):
    filtered_index = json.load(open(data_path.joinpath("cn_index_filtered.json"), "r", encoding="utf-8"))
    company_info = json.load(open(data_path.joinpath("cn_company.json"), "r", encoding="utf-8"))

    index_dir = data_path.joinpath("index_info")
    if not index_dir.exists():
        index_dir.mkdir()

    # 记录开始时间
    start_time = time.time()
    total_count = len(filtered_index)
    
    # 使用线程池并行抓取数据，最多5个线程
    with ThreadPoolExecutor(max_workers=15) as executor:
        # 提交所有任务
        future_to_index = {
            executor.submit(fetch_single_index_data, index, company_info): index 
            for index in filtered_index
        }
        
        # 处理完成的任务
        completed_count = 0
        for future in as_completed(future_to_index):
            completed_count += 1
            index = future_to_index[future]
            try:
                result = future.result()
                if result is not None:
                    # 保存结果
                    with open(index_dir.joinpath(f"{result['stockCode']}.pickle"), "wb") as f:
                        pickle.dump(result, f)
                    logging.info(f"已完成 {result['stockCode']} 的数据抓取和保存")
                else:
                    logging.error(f"未能成功抓取 {index['stockCode']} 的数据")
            except Exception as e:
                logging.error(f"处理 {index['stockCode']} 的结果时出错: {e}")
            
            # 计算进度信息
            elapsed_time = time.time() - start_time
            avg_time_per_item = elapsed_time / completed_count if completed_count > 0 else 0
            remaining_items = total_count - completed_count
            estimated_remaining_time = avg_time_per_item * remaining_items
            
            # 格式化时间显示
            elapsed_formatted = str(timedelta(seconds=int(elapsed_time)))
            remaining_formatted = str(timedelta(seconds=int(estimated_remaining_time)))
            
            # 打印进度信息
            logging.info(f"进度: {completed_count}/{total_count} ({completed_count/total_count*100:.1f}%) "
                        f"已用时: {elapsed_formatted} "
                        f"预计剩余: {remaining_formatted}")