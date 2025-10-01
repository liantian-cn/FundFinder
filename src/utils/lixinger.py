#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import json
import os

import requests

BASEURL = "https://open.lixinger.com/api/"

__all__ = ["query_json"]


def get_token():
    """
    获取token
    """
    token = os.getenv("LIXINGER_TOKEN")
    if token is not None:
        return token
    raise Exception("请配置环境变量LIXINGER_TOKEN")


def get_full_url(url_suffix):
    url_suffix = url_suffix.replace('.', '/')
    if url_suffix.startswith('/'):
        url_suffix = url_suffix[1:]
    return BASEURL + url_suffix


def query_json(url_suffix, query_params=None):
    if query_params is None:
        query_params = dict()
    if get_token() is None:
        raise Exception("token未设置")
    query_params["token"] = get_token()

    headers = {"Content-Type": "application/json"}
    response = requests.post(url=get_full_url(url_suffix), data=json.dumps(query_params), headers=headers)
    return response.json()
