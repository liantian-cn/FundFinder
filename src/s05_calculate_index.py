import json
import logging
import pickle
import pytz
from datetime import datetime, timedelta
from utils import retry, find_dict_by_field, get_dates_ranges
from utils import query_json
import pathlib
import pandas as pd

pd.set_option('display.width', 1000)
# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)


def calculate_index(data_path: pathlib.Path, config: dict):
    filtered_index = json.load(open(data_path.joinpath("cn_index_filtered.json"), "r", encoding="utf-8"))
    company_info = json.load(open(data_path.joinpath("cn_company.json"), "r", encoding="utf-8"))

    index_dir = data_path.joinpath("index_info")
    if not index_dir.exists():
        index_dir.mkdir()

    for index in filtered_index:
        with open(index_dir.joinpath(f"{index['stockCode']}.pickle"), "rb") as f:
            index_info = pickle.load(f)
        logging.info(f"正在计算{index['stockCode']}的指数信息")

        df = index_info["dataframe"]

        # 计算移动平均线
        ma_periods = [5, 10, 20, 30, 60, 120, 250]
        for period in ma_periods:
            df[f'{period}日均线'] = df['收盘价'].rolling(window=period).mean()

        # 计算布林线 (以20日周期为例)
        bb_period = config["bb_period"]
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
        with open(index_dir.joinpath(f"{index['stockCode']}.pickle"), "wb") as f:
            pickle.dump(index_info, f)

        logging.info(f"完成计算{index['stockCode']}的指数信息")
        # print(df[-30:])
        # return

    logging.info("所有指数计算完成")
