import json
import logging
import pickle
import pathlib
import numpy as np


def export_home(data_path: pathlib.Path, output_path: pathlib.Path, ):
    filtered_index = json.load(open(data_path.joinpath("cn_index_filtered.json"), "r", encoding="utf-8"))

    index_dir = data_path.joinpath("index_info")
    output_path = output_path.joinpath("index")

    result = []     

    for index in filtered_index:
        with open(index_dir.joinpath(f"{index['stockCode']}.pickle"), "rb") as f:
            index_info = pickle.load(f)
        # print(index_info)
        df = index_info["dataframe"]
        entry = df.to_dict('records')[-1]
        # print(df)
        entry["tracking_fund_count"] = len(index_info["tracking_fund"])
        entry["name"] = index_info["name"]

        backtest_stat = index_info["backtest_stat"]
        annual_returns = [stat["annual_return"] for stat in backtest_stat if stat["holding_days"] > 20]
        entry["backtest_avg"] = np.mean(annual_returns) if annual_returns else 0
        # print(entry)

        result.append(entry)

    # 处理NaN值，避免JSON序列化错误
    for item in result:
        for key, value in item.items():
            if isinstance(value, float) and np.isnan(value):
                item[key] = None

    with open(output_path.joinpath("home.json"), "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
    