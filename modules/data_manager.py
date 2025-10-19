#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据管理模块

该模块负责数据的保存和加载操作。
"""

import json
import pathlib
import logging


def save_data_to_json(data, file_path, encoding="utf-8"):
    """将数据保存为JSON文件。
    
    Args:
        data: 要保存的数据
        file_path: 文件路径
        encoding: 文件编码，默认为utf-8
    """
    try:
        with open(file_path, "w", encoding=encoding) as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info(f"数据已成功保存到 {file_path}")
    except Exception as e:
        logging.error(f"保存数据到 {file_path} 时出错: {e}")
        raise


def load_data_from_json(file_path, encoding="utf-8"):
    """从JSON文件加载数据。
    
    Args:
        file_path: 文件路径
        encoding: 文件编码，默认为utf-8
        
    Returns:
        加载的数据
    """
    try:
        with open(file_path, "r", encoding=encoding) as f:
            data = json.load(f)
        logging.info(f"成功从 {file_path} 加载数据")
        return data
    except FileNotFoundError:
        logging.warning(f"文件 {file_path} 不存在")
        raise
    except Exception as e:
        logging.error(f"从 {file_path} 加载数据时出错: {e}")
        raise