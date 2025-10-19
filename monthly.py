#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
月度数据更新脚本

该脚本负责每月更新中国A股指数、公司基础信息以及指数成分股和跟踪基金信息。
主要功能包括：
1. 获取所有A股指数基础信息
2. 获取所有A股公司基础信息
3. 获取每个指数的成分股及其权重
4. 获取跟踪每个指数的基金信息
5. 更新并保存相关数据到JSON文件

依赖:
- utils.py 中的工具函数
- config.json 配置文件
- 环境变量 LIXINGER_TOKEN (用于访问数据API)

使用方法:
直接运行此脚本即可执行月度数据更新任务。
"""

import json
import pathlib
import time
import logging

# 设置日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

from modules.index_data_fetcher import (
    fetch_cn_index,
    fetch_cn_company,
    update_index_info
)
from modules.data_manager import save_data_to_json, load_data_from_json
from modules.config_manager import load_config


BASE_DIR = pathlib.Path(__file__).parent

# 加载配置文件，如果不存在则使用空字典
config = load_config(BASE_DIR.joinpath("config.json"), {})


def main():
    """主函数，执行月度数据更新任务。"""
    logging.info("开始执行月度数据更新任务")
    
    try:
        # 获取所有A股指数基础信息并保存
        cn_index = fetch_cn_index()
        cn_index_file = BASE_DIR.joinpath("cn_index.json")
        save_data_to_json(cn_index, cn_index_file)

        # 获取所有A股公司基础信息并保存
        cn_company = fetch_cn_company()
        cn_company_file = BASE_DIR.joinpath("cn_company.json")
        save_data_to_json(cn_company, cn_company_file)

        # 更新所有指数的完整信息（成分股和跟踪基金）
        update_index_info(cn_index_file, cn_company)
        
        logging.info("月度数据更新任务执行完成")
    except Exception as e:
        logging.error(f"执行月度数据更新任务时出错: {e}")
        raise


if __name__ == '__main__':
    main()