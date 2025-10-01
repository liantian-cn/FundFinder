import pathlib
import json
from datetime import datetime
import pytz


def filter_cn_index(data_path: pathlib.Path, config: dict):
    original_index = json.load(open(data_path.joinpath("cn_index.json"), "r", encoding="utf-8"))

    # 白名单过滤规则
    filter_index = [item for item in original_index if item["stockCode"] in config["funds"]]

    # 黑名单过滤
    # filter_index = [item for item in original_index if item["stockCode"] not in config["black_list"]]

    # 过滤launchDate小于5年的
    # filter_index = [item for item in filter_index if (datetime.now(pytz.timezone("Asia/Shanghai")) - datetime.fromisoformat(item["launchDate"])).days > 365 * 10]

    json.dump(filter_index, open(data_path.joinpath("cn_index_filtered.json"), "w", encoding="utf-8"), indent=4, ensure_ascii=False)
