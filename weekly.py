#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
周度数据筛选脚本

该脚本负责每周对A股指数数据进行筛选，过滤掉不符合条件的指数。
主要筛选条件包括：
1. 指数成立时间需满3年
2. 指数必须有成分股信息
3. 指数必须有跟踪基金

筛选后的数据将保存到 cn_index_filtered.json 文件中，供其他模块使用。

依赖:
- cn_index.json: 包含完整指数数据的文件
- config.json: 配置文件（可选）

使用方法:
直接运行此脚本即可执行周度数据筛选任务。
"""

import json
import pathlib
import logging

# 设置日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

from modules.data_manager import load_data_from_json, save_data_to_json
from modules.config_manager import load_config
from modules.index_filter import filter_indices_by_criteria

BASE_DIR = pathlib.Path(__file__).parent

# 加载配置文件，如果不存在则使用空字典
config = load_config(BASE_DIR.joinpath("config.json"), {})


def main():
    """主函数，执行周度数据筛选任务。"""
    logging.info("开始执行周度数据筛选任务")
    
    try:
        # 加载指数数据
        cn_index_file = BASE_DIR.joinpath("cn_index.json")
        logging.info(f"正在加载指数数据文件: {cn_index_file}")
        cn_index = load_data_from_json(cn_index_file)
        logging.info(f"成功加载 {len(cn_index)} 个指数数据")
        
        # 筛选指数
        filtered_indices = filter_indices_by_criteria(cn_index)
        
        # 保存筛选结果
        output_file = BASE_DIR.joinpath("cn_index_filtered.json")
        logging.info(f"正在保存筛选结果到: {output_file}")
        save_data_to_json(filtered_indices, output_file)
        logging.info("筛选结果保存成功")
        
        logging.info("周度数据筛选任务执行完成")
    except Exception as e:
        logging.error(f"执行周度数据筛选任务时出错: {e}")
        raise


if __name__ == '__main__':
    main()