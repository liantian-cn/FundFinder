#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pathlib
import pytz
from datetime import datetime, timedelta

NOW = datetime.now(pytz.timezone("Asia/Shanghai"))

BASE_DIR = pathlib.Path(__file__).parent

config = json.load(open(BASE_DIR.joinpath("config.json"), encoding="utf-8"))


def main():
    cn_index = json.load(BASE_DIR.joinpath("cn_index.json").open(encoding="utf-8"))

    # 初筛
    for index in cn_index:
        index["enable"] = True
        # 未满3年的，过滤掉
        launchDate =  datetime.fromisoformat(index["launchDate"])
        if (NOW - launchDate).days < (365 * 3):
            index["enable"] = False
            continue

        if len(index["constituent_weightings"]) == 0:
            index["enable"] = False
            continue

        if len(index["tracking_fund"]) == 0:
            index["enable"] = False
            continue


    cn_index = [item for item in cn_index if item["enable"]]

    with open(BASE_DIR.joinpath("cn_index_filtered.json"), "w", encoding="utf-8") as f:
        json.dump(cn_index, f, indent=4, ensure_ascii=False)

if __name__ == '__main__':
    main()
    # 980028