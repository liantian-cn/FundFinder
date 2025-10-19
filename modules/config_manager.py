#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置管理模块

该模块负责处理配置文件的加载和管理。
"""

import json
import pathlib
import logging
import os


def load_config(config_path, default_config=None):
    """加载配置文件。
    
    Args:
        config_path: 配置文件路径
        default_config: 默认配置，当配置文件不存在时使用
        
    Returns:
        dict: 配置信息
    """
    try:
        with open(config_path, encoding="utf-8") as f:
            config = json.load(f)
        logging.info(f"成功加载配置文件 {config_path}")
        return config
    except FileNotFoundError:
        if default_config is not None:
            logging.warning(f"配置文件 {config_path} 不存在，使用默认配置")
            return default_config
        else:
            logging.error(f"配置文件 {config_path} 不存在")
            raise
    except Exception as e:
        logging.error(f"加载配置文件 {config_path} 时出错: {e}")
        raise


def get_api_token():
    """获取API令牌。
    
    Returns:
        str: API令牌
        
    Raises:
        Exception: 当环境变量LIXINGER_TOKEN未设置时抛出
    """
    token = os.getenv("LIXINGER_TOKEN")
    if token is not None:
        logging.debug("成功获取API令牌")
        return token
    
    logging.error("环境变量 LIXINGER_TOKEN 未设置")
    raise Exception("请配置环境变量LIXINGER_TOKEN")