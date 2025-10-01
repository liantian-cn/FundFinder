#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pathlib
from s01_cn_index import fetch_cn_index
from s02_filter_cn_index import filter_cn_index
from s03_cn_company import fetch_cn_company
from s04_fetch_index_info import fetch_index_info
from s05_calculate_index import calculate_index
from s06_export_to_js import export_to_js

BASE_DIR = pathlib.Path(__file__).parent.parent

config = json.load(open(BASE_DIR.joinpath("config.json"), encoding="utf-8"))


def init():
    # 初始化的信息，日常无需使用
    # 01
    # fetch_cn_index(BASE_DIR.joinpath("data"))
    # 02
    # filter_cn_index(BASE_DIR.joinpath("data"), config)
    # 获取公司数据
    # fetch_cn_company(BASE_DIR.joinpath("data"))
    return


def main():
    # 获取指数的所有信息
    # fetch_index_info(BASE_DIR.joinpath("data"))
    # 计算每个指数
    calculate_index(BASE_DIR.joinpath("data"), config)
    # # 导出到json文件
    export_to_js(BASE_DIR.joinpath("data"), BASE_DIR.joinpath("output"))
    return


if __name__ == '__main__':
    init()
    main()
