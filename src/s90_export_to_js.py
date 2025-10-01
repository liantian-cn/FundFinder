import json
import logging
import pickle
import pathlib


def export_to_js(data_path: pathlib.Path, output_path: pathlib.Path, ):
    filtered_index = json.load(open(data_path.joinpath("cn_index_filtered.json"), "r", encoding="utf-8"))

    index_dir = data_path.joinpath("index_info")
    output_path = output_path.joinpath("index")
    if not output_path.exists():
        output_path.mkdir()

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

    for index in filtered_index:
        with open(index_dir.joinpath(f"{index['stockCode']}.pickle"), "rb") as f:
            index_info = pickle.load(f)

        # 创建一个副本以避免修改原始数据
        df = index_info["dataframe"].copy()

        # 重命名列名为英文
        df.rename(columns=column_mapping, inplace=True)

        # 导出为JSON格式
        index_info["dataframe"] = json.loads(df.to_json(orient="records", indent=4))
        with open(output_path.joinpath(f"{index['stockCode']}.json"), "w", encoding="utf-8") as f:
            json.dump(index_info, f, ensure_ascii=False, indent=4)
    logging.info("所有指数导出完成")
